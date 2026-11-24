"""只读副本的物理 WAL 应用。

主库 checkpoint 后复制数据文件作为基准备份，副本从 ``checkpoint_lsn + 1``
开始读取主库 WAL，把页更新重放到自己的 Buffer Pool，并用主库事务状态控制
MVCC 可见性。记录可以直接来自同进程的 ``primary.wal``，也可以通过
``WALServer`` / ``WALClient`` 的 TCP 流读取；两种路径使用同一套物理重放。

当前约束：
- 基准备份要求主库没有活跃事务；否则复制到的是包含未提交版本的数据文件。
- 基准备份在 ``Database.snapshot`` / ``snapshot_file`` 的写锁内 checkpoint，
  写语句被串行化，读查询可继续。
- 副本假设单线程应用 WAL；查询与 apply 不应并发。
- 主库 WAL 的保留依赖 ``SegmentedWAL.retain_lsn``；旧单文件 WAL 没有该
  属性，只能用于测试或手动控制 checkpoint。
"""

from __future__ import annotations

import logging
import os
import shutil
import ssl
import threading
import time
import warnings
from collections.abc import Callable
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..engine.database import Database
from ..errors import NotSupportedError, StorageError, TransactionError
from ..storage.txn import TxnState
from ..storage.page import PageType
from ..storage.pager import DiskManager
from ..storage.wal import LogType, decode_page_image, decode_page_update
from ._atomic import atomic_replace
from .fence import PromotionFence
from .lease import Lease, LeaseFence, LeaseKeeper
from .quorum import HeartbeatLoop, QuorumCommitBarrier

_log = logging.getLogger(__name__)


class Replica:
    def __init__(
        self,
        path: str | Path,
        *,
        applied_lsn: int = 0,
        wal_segment_bytes: int = 1024 * 1024,
        max_pending_records: int = 100_000,
    ) -> None:
        self.path = Path(path)
        self.path.mkdir(parents=True, exist_ok=True)
        marker = self.path / "replica.lsn"
        marker_exists = marker.exists()
        next_lsn = applied_lsn + 1
        if marker_exists:
            try:
                parts = marker.read_text(encoding="utf-8").split()
                applied_lsn = int(parts[0] or "0")
                if len(parts) > 1:
                    next_lsn = int(parts[1])
                else:
                    # 旧 marker 只有已 apply 的记录 LSN，没有它的结束边界；
                    # 从 applied_lsn + 1 开始至少保证提升后的 LSN 不倒退。
                    next_lsn = applied_lsn + 1
                if applied_lsn < 0 or next_lsn <= applied_lsn:
                    raise ValueError("marker 边界非法")
            except (OSError, ValueError, IndexError) as exc:
                raise StorageError(
                    "副本复制进度文件损坏，拒绝从猜测的 LSN 继续",
                    detail=str(marker),
                ) from exc
        self.db = Database(
            self.path,
            read_only=True,
            checkpoint_wal_bytes=0,
            wal_segment_bytes=wal_segment_bytes,
        )
        self._applied_lsn = applied_lsn
        #: 提升成主库时下一条 WAL 记录应使用的 LSN。基准备份后是
        #: checkpoint_lsn + 1；每 apply 一批后推进到“最后一条记录 + 其编码长度”。
        self._next_lsn = max(applied_lsn + 1, int(next_lsn))
        # 上一批最后一条记录之后的字节边界；用于拒绝中间有洞的复制流。
        # 0 表示还没有在本进程内建立连续视图（首次 apply 允许从任意 LSN 开始，
        # 例如基准备份之后的第一条记录）。
        self._expected_end_lsn = 0
        # promote/step_down 重开数据库时沿用同一个分段大小，避免段文件混用
        # 默认 1MB 与用户配置。
        self._wal_segment_bytes = wal_segment_bytes
        # apply 与只读查询不能同时碰到 Buffer Pool / catalog。
        # 这把锁只保护"数据面"（一组 WAL 记录的应用 / 一次只读查询）；
        # 拉取循环的串行化在 ``_fetch_lock`` 上，避免两件事互相拖住。
        self._lock = threading.RLock()
        #: 串行化"拉取循环"（fetch + apply + ack）：同一时刻只允许一个线程
        #: 推进副本进度，否则两个拉取者会交错确认同一个 LSN。
        self._fetch_lock = threading.Lock()
        #: 一批 ``apply`` 内每组应用的记录数。组的边界是"给等待中的只读
        #: 查询让出锁"的窗口：批次很大时读者不必等整批应用完。
        self._apply_chunk_size = 512
        #: 跨批的"半个事务"暂存区（见 ``apply`` 的说明）。记录按 LSN 连续排列；
        #: ``_pending_end_lsn`` 是暂存区最后一条记录的 LSN，也用来当拉取游标——
        #: 否则下一批会从 applied_lsn 重新拉取同一段，永远推进不过去。
        self._pending_records: list = []
        self._pending_end_lsn = 0
        #: 暂存上限：单个事务超过它就报错，宁可让复制停住等人处理，
        #: 也不退化成"应用半个事务"。构造时可调（见 `__init__` 参数）。
        self._max_pending_records = max(1, int(max_pending_records))
        #: 正在等数据锁的只读查询数（无锁启发式计数，只用来决定要不要让出）。
        self._pending_reads = 0
        #: 正在进行"一致快照读"的线程数（``snapshot_read`` 上下文）。>0 时
        #: apply 不再在组边界交出锁：一次应用完剩余记录，读者就不会看到
        #: 跨表半应用的中间态。它只能由快照读者在持锁状态下改。
        self._snapshot_depth = 0
        self._lease_keeper: LeaseKeeper | None = None
        self._heartbeat: HeartbeatLoop | None = None
        #: 本副本对应的仲裁节点；用来把复制进度（match_lsn）报给 leader。
        self._quorum_node = None
        #: 同步复制的提交屏障（未开启时为 None）。
        self._commit_barrier: QuorumCommitBarrier | None = None
        #: 最近一次推送被 peer 拒绝的原因：``{peer_id: reason}``。
        #: ``rebuild_required`` 的"日志分歧"条目意味着该 peer 与本主库在
        #: 同一 LSN 上有两条不同历史，需要重新基准备份才能继续同步。
        self.last_push_rejections: dict[str, str] = {}
        #: 本节点作为 leader 时服务的基准备份端点 ``(host, port)``；非空时
        #: 随 append_entries 发给 follower，follower 判定历史分叉后会据此
        #: 自动重建（见 :meth:`_schedule_rebuild`）。
        self.rebuild_from: tuple[str, int] | None = None
        #: 自动重建端点要求的共享 token（与 rebuild_from 配套）。
        self.rebuild_token: str | None = None
        #: 最近一次自动重建记录（本节点作为 follower 重建自己），成功时
        #: ``{host, port, prev_lsn, applied_lsn}``，失败原因在
        #: ``last_rebuild_error``。
        self.last_rebuild: dict[str, Any] | None = None
        self.last_rebuild_error: str | None = None
        #: 全量重建时旧数据文件的保存位置（空串表示没有发生全量重建）。
        self.last_rebuild_preserved: str = ""
        #: 长连接复用：``replicate_from_server(keepalive=True)`` 时缓存一个
        #: ``WALClient``；参数（host/port/token/compress/binary）变化就换新的。
        #: 每批一次 TCP 握手在跨网络追平时是实打实的 RTT 开销，本机回环则
        #: 可以忽略，所以默认关闭。
        self._stream_client: Any | None = None
        self._stream_key: tuple[Any, ...] | None = None
        self._rebuild_lock = threading.Lock()
        self._rebuild_in_flight = False
        #: 推送退避：``{peer_id: 可重试的 monotonic 时刻}``。持续拒绝
        #: （无 applier / 重建反复失败）的 peer 在退避期内跳过推送。
        self._push_backoff: dict[str, float] = {}
        #: ``step_down()`` 的观察者（``ReplicationMonitor`` 用它更新状态机）。
        self._step_down_listeners: list[Callable[[str], None]] = []
        if not marker_exists:
            self._write_marker()

    # ------------------------------------------------------------------

    @classmethod
    def bootstrap(cls, primary: Database, path: str | Path) -> "Replica":
        """从主库的 checkpoint 基准备份创建副本。

        复制前先让主库 checkpoint，保证数据页与 pageLSN 一致；基准备份不
        拷贝 WAL，副本只应用 checkpoint 之后的记录。
        """
        if primary.txns.active_xids:
            raise TransactionError(
                "基准备份要求主库没有活跃事务",
                hint="先让所有事务 COMMIT / ROLLBACK，再 checkpoint 和复制",
            )
        data, base_lsn = primary.snapshot()
        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        data_path = target / "lumendb.data"
        if data_path.exists():
            raise TransactionError(
                "副本目录已经有数据文件",
                hint="换一个空目录，或先手动确认旧副本可以删除",
            )
        data_path.write_bytes(data)
        retain = getattr(primary.wal, "retain_lsn", None)
        if retain is not None:
            primary.wal.retain_lsn = base_lsn
        return cls(target, applied_lsn=base_lsn)

    @classmethod
    def bootstrap_from_server(
        cls,
        host: str,
        port: int,
        path: str | Path,
        *,
        timeout: float = 10.0,
        token: str | None = None,
        compress: bool = False,
        binary_records: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
    ) -> "Replica":
        """通过 TCP 拉取主库基准备份并打开副本。"""
        from .wal_stream import WALClient

        target = Path(path)
        target.mkdir(parents=True, exist_ok=True)
        data_path = target / "lumendb.data"
        if data_path.exists():
            raise TransactionError(
                "副本目录已经有数据文件",
                hint="换一个空目录，或先手动确认旧副本可以删除",
            )
        client = WALClient(
            host, port, timeout=timeout, token=token, compress=compress,
            ssl_context=ssl_context, server_hostname=server_hostname,
            replica_id=replica_id, binary_records=binary_records,
        )
        base_lsn = client.backup_to(data_path)
        if replica_id is not None:
            client.ack(base_lsn)
        return cls(target, applied_lsn=base_lsn)

    @classmethod
    def rejoin_from_server(
        cls,
        host: str,
        port: int,
        path: str | Path,
        *,
        archive_dir: str | Path | None = None,
        timeout: float = 10.0,
        token: str | None = None,
        compress: bool = False,
        binary_records: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
    ) -> tuple["Replica", Path | None]:
        """把曾当过主库/旧副本的本地目录归档，全量基准备份后作为只读副本返回。

        为什么不能走页级增量重建（``rebase_from_server``）：增量重建只覆盖
        "新主库 pageLSN 更新过的页"。旧主库如果留下**分叉尾巴**（崩溃前已提交、
        但从未复制出去的事务），它写过的页在新主库侧根本没被改过，增量重建
        不会覆盖它们 —— 副本就会带着新主库不认的行继续服务，读出来的数据比
        集群的真实前缀多。所以重新加入必须**全量**。

        归档而不是删除：旧目录整体改名为 ``<name>.stale-<YYYYMMDD-HHMMSS>``
        （给了 ``archive_dir`` 就移进去），现场保留供事后审计、人工捞数据。
        调用方必须先确保本地句柄已经关闭（``step_down()`` 或 ``close()``）。

        返回值是 ``(新副本, 归档目录或 None)``。
        """
        import shutil

        target = Path(path)
        archived: Path | None = None
        if target.exists() and any(target.iterdir()):
            stamp = time.strftime("%Y%m%d-%H%M%S")
            base = Path(archive_dir) if archive_dir is not None else target.parent
            base.mkdir(parents=True, exist_ok=True)
            dest = base / f"{target.name}.stale-{stamp}"
            index = 1
            while dest.exists():
                index += 1
                dest = base / f"{target.name}.stale-{stamp}-{index}"
            shutil.move(str(target), str(dest))
            archived = dest
        replica = cls.bootstrap_from_server(
            host,
            port,
            target,
            timeout=timeout,
            token=token,
            compress=compress,
            binary_records=binary_records,
            ssl_context=ssl_context,
            server_hostname=server_hostname,
            replica_id=replica_id,
        )
        return replica, archived

    # ------------------------------------------------------------------

    @property
    def applied_lsn(self) -> int:
        with self._lock:
            return self._applied_lsn

    @property
    def _fetch_cursor(self) -> int:
        """下一批从哪里开始拉：**暂存区里的记录也算已经取到**。

        用 ``applied_lsn`` 当游标的话，超长事务会卡死：第一批全在事务中间、
        一条都没应用，applied_lsn 不动，下一次又从同一条开始拉，永远走不到
        COMMIT。所以游标要取 applied 与 pending 的较大者。
        """
        return max(self._applied_lsn, self._pending_end_lsn)

    def lag_lsn(self, primary: Database) -> int:
        """尚未应用的 WAL 字节数；没有待应用记录时返回 0。"""
        with self._lock:
            if next(primary.wal.iter_records(self._applied_lsn + 1), None) is None:
                return 0
            return max(1, primary.wal.next_lsn - self._applied_lsn)

    def lag_from_server(
        self,
        host: str,
        port: int,
        *,
        timeout: float = 10.0,
        token: str | None = None,
        compress: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
    ) -> int:
        """通过 TCP 读取主库当前 LSN，计算尚未应用的字节数。"""
        from .wal_stream import WALClient

        with self._lock:
            status = WALClient(
                host, port, timeout=timeout, token=token, compress=compress,
                ssl_context=ssl_context, server_hostname=server_hostname,
                replica_id=replica_id,
            ).status()
            lag = status["current_lsn"] - self._applied_lsn
            return max(0, lag)

    def wait_until(
        self,
        target_lsn: int,
        host: str,
        port: int,
        *,
        timeout: float = 10.0,
        poll: float = 0.001,
        max_records: int = 1000,
        token: str | None = None,
        compress: bool = False,
        binary_records: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
    ) -> bool:
        """阻塞到副本已应用 ``target_lsn``（或超时），返回是否到达。

        用途是"读己之写"：客户端写入后重定向到副本，副本可能还没追平那个
        LSN；先 ``wait_until(lsn)`` 再查，就不会读到旧前缀。

        ``target_lsn`` 是主库返回给客户端的提交 LSN（例如 ``wal.last_lsn``
        或事务的 ``last_lsn``）。返回 True 表示 ``applied_lsn >= target_lsn``。
        """
        import time as _time

        deadline = _time.monotonic() + max(0.0, timeout)
        while True:
            if self.applied_lsn >= target_lsn:
                return True
            applied = self.replicate_from_server(
                host,
                port,
                max_records=max_records,
                timeout=timeout,
                token=token,
                compress=compress,
                binary_records=binary_records,
                ssl_context=ssl_context,
                server_hostname=server_hostname,
                replica_id=replica_id,
            )
            if applied:
                continue
            if self.applied_lsn >= target_lsn:
                return True
            if _time.monotonic() >= deadline:
                return False
            _time.sleep(max(0.0, poll))

    def apply_from_primary(self, primary: Database) -> int:
        """读取主库 WAL 中尚未应用的部分，返回本次应用的记录数。"""
        with self._fetch_lock:
            base_lsn = getattr(primary.wal, "base_lsn", 0)
            if base_lsn and self._applied_lsn + 1 < base_lsn:
                raise StorageError(
                    "副本落后于主库已回收的 WAL，需要重新基准备份",
                    detail=f"applied_lsn={self._applied_lsn} base_lsn={base_lsn}",
                )
            records = primary.wal.iter_records(self._fetch_cursor + 1)
            return self.apply(records, primary=primary)

    def replicate_from_server(
        self,
        host: str,
        port: int,
        *,
        max_records: int = 1000,
        timeout: float = 10.0,
        token: str | None = None,
        compress: bool = False,
        binary_records: bool = False,
        keepalive: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
    ) -> int:
        """通过 TCP 拉取一批 WAL 记录并应用，返回本次应用条数。

        ``binary_records=True`` 让主库把每条 WAL 记录编成 ``B`` 前缀的二进制定长
        帧（无 base64、无 JSON 字段名），非记录消息仍是 JSON 行。
        """
        with self._fetch_lock:
            from .wal_stream import WALClient

            key = (
                host, int(port), timeout, token, compress, binary_records,
                id(ssl_context), server_hostname, replica_id,
            )
            if keepalive and self._stream_client is not None:
                if self._stream_key != key:
                    self._release_stream_client()
                client = self._stream_client
            if not keepalive or self._stream_client is None:
                client = WALClient(
                    host, port, timeout=timeout, token=token, compress=compress,
                    ssl_context=ssl_context, server_hostname=server_hostname,
                    replica_id=replica_id, binary_records=binary_records,
                    keepalive=keepalive,
                )
                if keepalive:
                    self._stream_client = client
                    self._stream_key = key
            try:
                records = client.fetch(self._fetch_cursor, max_records=max_records)
            except Exception:
                # 长连接出错就丢掉缓存，下一次从新连接开始；错误照常抛给
                # 调用方（监控层负责计数与退避）。
                if keepalive:
                    self._release_stream_client()
                raise
            applied = self.apply(records, primary=None)
            if applied:
                client.ack(self._applied_lsn)
            return applied

    def apply(self, records, *, primary: Database | None = None) -> int:
        """应用一批 WAL 记录，返回本次**真正应用**的条数。

        ## 半事务暂存区

        ``max_records`` 是有界的，但一个事务可以比它更大：一次 2000 行批量
        插入会产生几千条"表页一条 + 每个索引页各一条"的记录。早期实现遇到
        "整批都没有 COMMIT/ABORT"时退回批尾切组，于是超长事务必然被切成两半，
        读者在中间会看到顺序扫描与索引扫描对不上（严重时 BTreeInvariantError）。

        现在的做法：**没有事务边界的记录一条都不应用**，先放进
        ``_pending_records``；下一批到达后与暂存区拼接，找到边界再一起应用。
        暂存区上限 ``_max_pending_records``，超过就报错——宁可让复制停住等人
        处理，也不退化成撕裂应用。

        拉取游标必须用 ``_fetch_cursor``（applied 与 pending 的较大者），
        否则第一批全在事务中间、applied_lsn 不动，会永远重拉同一段。

        ## 其余语义不变

        批次按 ``_apply_chunk_size`` 分组，组间释放数据锁，让等待中的只读
        查询不必等整批应用完；组边界一定落在事务边界上。组间重新校验数据库
        句柄，``promote`` / ``step_down`` / ``rebase_from_server`` 换掉 ``self.db``
        时本批中止，已应用记录保持、marker 不推进，重放由 pageLSN 幂等兜底。
        """
        database = self.db
        with self._lock:
            buffered = self._pending_records
            self._pending_records = []
            self._pending_end_lsn = 0
        incoming = list(records)
        if buffered:
            # 调用方可能从 applied_lsn 重取（重试 / 换连接），把暂存区已覆盖的丢掉。
            incoming = [r for r in incoming if r.lsn > buffered[-1].lsn]
            todo = buffered + incoming
        else:
            todo = incoming
        # 已经应用过的记录也过滤掉：重取批次里的 COMMIT 会被当成"新边界"，
        # 把后面真正的半个事务误判成可以应用。
        todo = [r for r in todo if r.lsn > self._applied_lsn]
        total = len(todo)
        if not total:
            return 0

        # 批里最后一个事务边界；它后面的记录属于下一个还没结束的事务。
        boundary = 0
        for index, record in enumerate(todo):
            if record.rtype in (LogType.COMMIT, LogType.ABORT):
                boundary = index + 1
        carry = todo[boundary:]
        if boundary == 0:
            if len(todo) > self._max_pending_records:
                raise StorageError(
                    "单个事务超过副本暂存上限，无法原子应用",
                    detail=(
                        f"pending={len(todo)} cap={self._max_pending_records}；"
                        "调大 Replica(max_pending_records=...)，或把事务拆小"
                    ),
                )
            with self._lock:
                self._pending_records = todo
                self._pending_end_lsn = todo[-1].lsn
            return 0

        todo = todo[:boundary]
        count = 0
        chunk = max(1, int(self._apply_chunk_size))
        start = 0
        total = len(todo)
        while start < total:
            # 切点必须落在事务结束（COMMIT/ABORT）之后：一条 DML 的 WAL
            # 是"表页记录 + 各索引页记录"，切在事务中间会让读者看到
            # "表页已应用、索引页未应用"（docs/replication-correctness.md）。
            size = self._transaction_cut(todo, start, total, chunk)
            with self._lock:
                if self.db is not database or getattr(database, "_closed", False):
                    # 句柄已换：已应用的保持，剩余记录（含 carry）放回暂存区，
                    # 由新句柄按 pageLSN 幂等重放。
                    keep = todo[start:] + carry
                    if keep:
                        self._pending_records = keep
                        self._pending_end_lsn = keep[-1].lsn
                    return count
                if self._snapshot_depth:
                    # 有读者在要"跨语句一致视图"：把剩余记录一次应用完，
                    # 不在这里把锁交出去，否则读者会看到跨表半应用的中间态。
                    size = total - start
                count += self._apply_records(todo[start : start + size], primary)
            # 有只读查询在等锁时给一个让出窗口；没有读者就不减速。
            # ``RLock`` 没有公平性：只 sleep(0) 的话应用线程常常立刻把锁
            # 抢回去，等待中的读者可能整批都轮不到；1ms 的窗口足够一次
            # SELECT 进入临界区，而只在"确实有人在等"时才付这个代价。
            if self._pending_reads > 0 and start + size < total:
                time.sleep(0.001)
            start += size
        if carry:
            with self._lock:
                self._pending_records = carry
                self._pending_end_lsn = carry[-1].lsn
        if count:
            with self._lock:
                if self.db is database:
                    # 先把页写下去，再推进副本 LSN；崩溃后最多重放一段记录，
                    # pageLSN 会让重复应用变成幂等操作。
                    self.db.bp.flush_all()
                    self.db.catalog.load()
                    self._write_marker()
                    if primary is not None:
                        retain = getattr(primary.wal, "retain_lsn", None)
                        if retain is not None:
                            primary.wal.retain_lsn = self._applied_lsn
        return count

    def _transaction_cut(self, todo, start: int, total: int, chunk: int) -> int:
        """返回从 ``start`` 开始的一段长度，使其结束在事务边界上。

        优先在 ``chunk`` 窗口内找最后一个 COMMIT/ABORT；窗口内没有，就向后
        延伸到最近的边界（保证一个事务不会被切成两半）。

        ``apply`` 现在只会把**到最后一个边界为止**的记录交给它，所以这里的
        "整段都没有边界就退回段尾"是防御性分支：正常路径不该走到。
        """
        limit = min(total, start + chunk)
        cut = None
        for index in range(start, limit):
            if todo[index].rtype in (LogType.COMMIT, LogType.ABORT):
                cut = index + 1
        if cut is not None:
            return cut - start
        for index in range(limit, total):
            if todo[index].rtype in (LogType.COMMIT, LogType.ABORT):
                return index + 1 - start
        return total - start

    def _apply_records(self, records, primary: Database | None) -> int:
        """在已持有数据锁的前提下应用一段记录（连续性与幂等检查在内）。"""
        count = 0
        for record in records:
            if record.lsn <= self._applied_lsn:
                continue
            if (
                self._expected_end_lsn
                and record.lsn != self._expected_end_lsn
            ):
                raise StorageError(
                    "复制记录不连续，拒绝跳过缺失的 WAL",
                    detail=(
                        f"expected_lsn={self._expected_end_lsn} "
                        f"got_lsn={record.lsn}"
                    ),
                )
            if record.rtype == LogType.PAGE_IMAGE:
                self._apply_page_image(record)
            elif record.rtype in (LogType.UPDATE, LogType.CLR):
                self._apply_page_update(record, primary)
            elif record.rtype == LogType.BEGIN:
                self.db.txns.note_replica_state(record.xid, TxnState.ACTIVE)
            elif record.rtype == LogType.COMMIT:
                self.db.txns.note_replica_state(record.xid, TxnState.COMMITTED)
            elif record.rtype == LogType.ABORT:
                self.db.txns.note_replica_state(record.xid, TxnState.ABORTED)
            # CHECKPOINT 只对主库自己的恢复有意义；副本按 LSN 继续追。
            self._applied_lsn = record.lsn
            self._expected_end_lsn = record.lsn + len(record.encode())
            self._next_lsn = self._expected_end_lsn
            count += 1
        return count

    def _apply_page_image(self, record) -> None:
        page_id, image = decode_page_image(record.payload)
        self.db.disk.ensure_page(page_id)
        page = self.db.bp.fetch_page(page_id)
        try:
            if page.page_lsn < record.lsn:
                page.buf[:] = image
                page.page_lsn = record.lsn
                self.db.bp.mark_dirty(page)
        finally:
            self.db.bp.unpin(page_id)

    def _apply_page_update(self, record, primary: Database | None) -> None:
        page_id, offset, _old, new = decode_page_update(record.payload)
        self.db.disk.ensure_page(page_id)
        page = self.db.bp.fetch_page(page_id)
        try:
            # 新页的“分配 + 初始化”不是一条独立 WAL 记录：主库只把后续
            # 页内 diff 写进 WAL。副本看到 FREE 页时，先向主库取一次完整
            # 页镜像（在真实网络实现里就是一个 PAGE_IMAGE 请求），再应用
            # 当前 diff。已有页则只按 pageLSN 应用 diff。
            if page.page_type == PageType.FREE and primary is not None:
                primary_page = primary.bp.fetch_page(page_id)
                try:
                    page.buf[:] = primary_page.buf
                    page.page_lsn = primary_page.page_lsn
                finally:
                    primary.bp.unpin(page_id)
                self.db.bp.mark_dirty(page)
            if page.page_lsn < record.lsn:
                page.buf[offset : offset + len(new)] = new
                page.page_lsn = record.lsn
                self.db.bp.mark_dirty(page)
        finally:
            self.db.bp.unpin(page_id)

    def _write_marker(self) -> None:
        marker = self.path / "replica.lsn"
        tmp = self.path / "replica.lsn.tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            fh.write(f"{self._applied_lsn}\n{self._next_lsn}\n")
            fh.flush()
            os.fsync(fh.fileno())
        atomic_replace(tmp, marker)
        # 目录项本身也要落盘，否则崩溃后可能看到旧的 marker（会导致重放，
        # 但不会丢已 flush 的页）；在没有目录 fsync 的平台上跳过。
        try:
            dir_fd = os.open(self.path, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass
        # 把"本副本真正复制到哪"告诉仲裁节点：leader 的心跳响应/水位计算
        # 依赖这个值，而它和"听到过"的 last_lsn 不是一回事。
        node = getattr(self, "_quorum_node", None)
        if node is not None:
            try:
                term = getattr(getattr(node, "state", None), "term", 0)
                node.observe_applied(self._applied_lsn, term=term)
            except (AttributeError, OSError):
                pass

    def _current_lsn(self) -> int:
        """当前写路径真正到过的 LSN。

        提升之后 ``_applied_lsn`` 只记录复制 apply 的进度，正常 ``db.execute``
        不再更新它；心跳和租约如果继续读这个旧值，会把 leader 自己报成 0，
        多数派水位永远算不上去。只读句柄仍用复制标记，避免把 WAL 中尚未
        apply 的尾部误报成已持有。
        """
        db = getattr(self, "db", None)
        local = int(getattr(self, "_applied_lsn", 0))
        if db is None or getattr(db, "_read_only", True):
            return local
        try:
            return max(local, int(db.wal.last_lsn))
        except (AttributeError, OSError):
            return local

    def _adopt_local_lsn(self) -> int:
        """把可写句柄的 WAL 进度吸收进复制标记。

        提升后本节点成为主库，写入只推进 ``db.wal.last_lsn``；降级/关闭前
        必须把它写回 ``replica.lsn``，否则重启后再次提升会拿旧进度参加
        竞选，把自己误判成落后副本。
        """
        current = self._current_lsn()
        if current > int(getattr(self, "_applied_lsn", 0)):
            self._applied_lsn = current
            wal = getattr(self.db, "wal", None)
            next_lsn = int(getattr(self, "_next_lsn", current + 1))
            if wal is not None:
                next_lsn = max(next_lsn, int(getattr(wal, "next_lsn", 0)))
            self._next_lsn = next_lsn
            # 写路径的 WAL 进度不是复制 apply 的连续边界。
            self._expected_end_lsn = 0
            self._write_marker()
        return int(getattr(self, "_applied_lsn", 0))

    def attach_quorum(self, quorum) -> None:
        """绑定仲裁节点：之后每次 apply 推进都会 `observe_applied`。"""
        if not hasattr(quorum, "observe_applied"):
            return
        self._quorum_node = quorum
        term = getattr(getattr(quorum, "state", None), "term", 0)
        quorum.observe_applied(self._applied_lsn, term=term)
        setter = getattr(quorum, "set_log_applier", None)
        if setter is not None:
            # append_entries 的条目直接走同一套物理重放，不另造一条执行路径。
            setter(self.apply)
        if hasattr(quorum, "rebuild_callback"):
            # 本节点作为 follower 被 leader 判出历史分叉时，用 leader 给的
            # 基准备份端点自动重建自己（后台执行、去重）。
            quorum.rebuild_callback = self._schedule_rebuild

    def _schedule_rebuild(self, host: str, port: int, prev_lsn: int) -> None:
        """拼接点分叉后的自愈：从 leader 重新基准备份（后台线程，去重）。

        由 ``QuorumNode.append_entries`` 的分叉分支回调；重建期间在途的
        apply 会因锁竞争而排队，leader 侧短暂超时后下一轮心跳会重试。
        """
        # 已提升（可写）的节点绝不能被"重建"覆盖数据目录：重建会关闭并
        # 重开句柄、用 leader 的基准备份替换数据文件，而本地可能已经有
        # 对外确认过的写入。出现"新 leader 推分叉日志"时正确顺序是：
        # 先降级（step_down）再以 follower 身份重建。
        if not getattr(getattr(self, "db", None), "_read_only", True):
            _log.warning(
                "本节点当前可写（已提升），拒绝被重建（lsn=%s from %s:%s）",
                prev_lsn, host, port,
            )
            self.last_rebuild_error = "promoted: refuse rebuild while writable"
            return
        with self._rebuild_lock:
            if self._rebuild_in_flight:
                return
            self._rebuild_in_flight = True

        def _rebuild() -> None:
            try:
                _log.warning(
                    "检测到历史分叉（lsn=%s），从 %s:%s 重新基准备份",
                    prev_lsn, host, port,
                )
                try:
                    self.rebase_from_server(
                        host, int(port), token=self.rebuild_token
                    )
                except Exception as exc:  # noqa: BLE001 - 增量重建失败才回退
                    # 页级增量在"本地文件损坏 / 主库已回收旧页"时无法完成，
                    # 此时只剩全量重建一条路。全量重建会保留旧文件取证明。
                    _log.warning(
                        "增量重建失败（%s），回退到全量重建", exc
                    )
                    self.rebuild_from_scratch(
                        host, int(port), token=self.rebuild_token
                    )
                with self._lock:
                    applied = self._applied_lsn
                node = getattr(self, "_quorum_node", None)
                reset = getattr(
                    getattr(node, "state", None), "reset_progress", None
                )
                if reset is not None:
                    # 旧历史的 match/任期分段全部作废，否则 leader 下一轮
                    # 推送会按旧位置发，永远 prefix_mismatch。
                    reset(applied)
                self.last_rebuild = {
                    "host": host,
                    "port": int(port),
                    "prev_lsn": int(prev_lsn),
                    "applied_lsn": applied,
                }
                self.last_rebuild_error = None
                _log.info("分叉重建完成，新基线 lsn=%s", applied)
            except Exception as exc:  # noqa: BLE001 - 重建失败不能打死后台线程
                _log.exception("分叉重建失败: %s", exc)
                self.last_rebuild_error = str(exc)
            finally:
                with self._rebuild_lock:
                    self._rebuild_in_flight = False

        threading.Thread(
            target=_rebuild, name="lumendb-replica-rebuild", daemon=True
        ).start()

    def push_entries_to_peers(
        self,
        quorum,
        *,
        max_entries: int = 1000,
    ) -> dict[str, int]:
        """把本地 WAL 的下一批记录通过 append_entries 推给 peers。

        leader 侧既可显式调用（先跑一轮 ``heartbeat_round`` 填好每个 peer 的
        ``last_matches``），也会被提升后的 ``HeartbeatLoop`` 每轮自动调用。
        只处理下一批；失败/前缀不匹配时返回 0 并留下 peer 的已知位置，交由
        下一次调用或 WAL 流回填。

        每个 batch 附带逐条任期（``entry_terms``）与 leader 已知的
        ``prev_lsn`` 处任期：follower 在同一 LSN 上看到不同任期会拒绝并
        要求重建（``rebuild_required``），拒绝原因记在
        ``last_push_rejections``。这里没有分歧尾巴截断——重建才是修复手段。
        基准备份流不携带任期，follower 对那段历史没有任期知识，首次接触
        会信任 leader 的拼接点任期（参见 ``QuorumNode.append_entries``）。
        """
        db = getattr(self, "db", None)
        if db is None or getattr(db, "_read_only", True):
            raise TransactionError("只有可写主库才能推送 append_entries")
        # 自动推送也必须经过写围栏：租约丢失/观察到更高任期后，旧 leader
        # 不能继续把本地日志推给副本。
        db.assert_writable()
        limit = max(1, min(1000, int(max_entries)))
        node = self._quorum_node
        if node is None or not hasattr(node, "last_matches"):
            raise TransactionError("没有绑定仲裁节点，无法推送 append_entries")
        from .quorum import QuorumClient
        from .wal_stream import _record_to_json

        pushed: dict[str, int] = {}
        now = time.monotonic()
        rebuild_from = self.rebuild_from
        for peer_id, (host, port) in dict(getattr(node, "peers", {})).items():
            if peer_id == node.node_id:
                continue
            if peer_id not in node.last_matches:
                continue
            # 持续拒绝的 peer 退避：无 applier（配置错误）或重建反复失败的
            # peer 每轮心跳空推 1000 条纯属浪费；退避期内跳过，成功推进后
            # 立即恢复。
            skip_until = self._push_backoff.get(peer_id, 0.0)
            if now < skip_until:
                continue
            prev_lsn = int(node.last_matches[peer_id])
            # 拼接点任期优先用 leader 自己的知识：peer 上报的 match_term
            # 是"最近一批 leader 任期"的近似值，真正的分歧检测要靠
            # "同一 LSN 的任期是否相同"。
            known_term = node.state.term_at(prev_lsn)
            prev_term = (
                int(known_term)
                if known_term is not None
                else int(node.last_match_terms.get(peer_id, 0))
            )
            records = list(db.wal.iter_records(prev_lsn + 1))[:limit]
            if not records:
                # peer 可能落后到本地 WAL 已无法连续供应的位置：本地拿不出
                # 拼接点之后的记录（提升后新 WAL 从 applied 边界开始，或
                # 旧段已回收），重发空批只会让 follower 干等。用本地已应用
                # 位置判断：prev 落后于它且 WAL 没有可推的记录 = 缺口，
                # 主动发一次 rebuild 信号让 follower 走基准备份自愈。
                applied = int(getattr(self, "_applied_lsn", 0) or 0)
                if applied > prev_lsn and rebuild_from is not None:
                    QuorumClient.append_entries(
                        host,
                        port,
                        node.state.term,
                        node.node_id,
                        prev_lsn,
                        prev_term,
                        [],
                        rebuild_host=rebuild_from[0],
                        rebuild_port=rebuild_from[1],
                        rebuild_hint=True,
                        timeout=node.timeout,
                        token=getattr(node, "token", None),
                    )
                    self._push_backoff[peer_id] = now + 2.0
                continue
            entries = [_record_to_json(record) for record in records]
            entry_terms = [node.state.term_at(record.lsn) for record in records]
            try:
                response = QuorumClient.append_entries(
                    host,
                    port,
                    int(node.state.term),
                    node.node_id,
                    prev_lsn,
                    prev_term,
                    entries,
                    commit_lsn=int(node.quorum_commit_lsn),
                    entry_terms=entry_terms,
                    rebuild_host=rebuild_from[0] if rebuild_from else None,
                    rebuild_port=rebuild_from[1] if rebuild_from else None,
                    timeout=node.timeout,
                    token=node.token,
                )
            except (OSError, StorageError) as exc:
                # 单个 peer 超时/断连不能中断整轮推送：否则它后面的 peer
                # 永远轮不到，且直接调用方（非心跳线程）会收到裸 TimeoutError。
                # 当作一次拒绝记下来并退避，下一轮再试。
                reason = f"append_entries 传输失败: {type(exc).__name__}: {exc}"
                if self.last_push_rejections.get(peer_id) != reason:
                    _log.warning(
                        "append_entries 发往 %s 失败: %s (prev_lsn=%s)",
                        peer_id,
                        exc,
                        prev_lsn,
                    )
                self.last_push_rejections[peer_id] = reason
                self._push_backoff[peer_id] = time.monotonic() + 2.0
                continue
            if max(0, int(response.get("term", 0))) > int(node.state.term):
                # 有 peer 已进入更高任期：本节点已经过期，不再把同一批
                # 旧任期的条目推给其他 peer；任期推进交给下一轮心跳。
                break
            if response.get("ok"):
                node.last_matches[peer_id] = int(response.get("match_lsn", 0))
                node.last_match_terms[peer_id] = int(
                    response.get("match_term", 0)
                )
                pushed[peer_id] = len(records)
                self.last_push_rejections.pop(peer_id, None)
                self._push_backoff.pop(peer_id, None)
            else:
                reason = str(response.get("reason", "unknown"))
                if self.last_push_rejections.get(peer_id) != reason:
                    # 无人值守运行时的可观测性：拒绝原因首次出现或变化时
                    # 记一条 warning，避免每轮心跳刷屏。
                    _log.warning(
                        "append_entries 被 %s 拒绝: %s (prev_lsn=%s)",
                        peer_id, reason, prev_lsn,
                    )
                self.last_push_rejections[peer_id] = reason
                if response.get("reason") == "prefix_mismatch":
                    # 只记录对方真实前缀，下一次从那里再试；不在这里截断本地
                    # 日志（避免把 leader 自己的已提交尾巴误删）。
                    node.last_matches[peer_id] = int(response.get("match_lsn", 0))
                    node.last_match_terms[peer_id] = int(
                        response.get("match_term", 0)
                    )
                    self._push_backoff.pop(peer_id, None)
                elif (
                    response.get("rebuild_required")
                    or reason == "本节点没有绑定日志重放函数"
                ):
                    # rebuild_required（日志分歧）时保持 last_matches 不动：
                    # 该 peer 需要重新基准备份，盲目重发只会反复被拒；无
                    # applier 的 peer 是配置错误。两者都退避几秒，别让每轮
                    # 心跳空推 1000 条。重建成功/peer 修复后由成功路径清除。
                    backoff = 2.0 if response.get("rebuild_required") else 5.0
                    self._push_backoff[peer_id] = time.monotonic() + backoff
        if pushed:
            # peer 已经真实 apply：立刻把它的 match_lsn 计入多数派水位，
            # 同步复制的屏障不用再等下一轮心跳。
            refresh = getattr(node, "refresh_commit_lsn", None)
            if refresh is not None:
                refresh(self._current_lsn())
        return pushed

    # ------------------------------------------------------------------

    @contextmanager
    def snapshot_read(self):
        """把一段多条语句的只读工作固定在同一个已应用前缀上。

        ``apply`` 默认按 ``_apply_chunk_size`` 分组、组间释放数据锁（这样长
        批不会把读者饿死）。单条语句因此是原子的，但"先查 A 表、再查 B 表"
        会跨过组边界：如果中间刚好应用了"只写了 A 表"的记录，读者就会看到
        A 已更新、B 还没更新的中间态。

        用法::

            with replica.snapshot_read():
                a = replica.execute_one("SELECT COUNT(*) FROM a")
                b = replica.execute_one("SELECT COUNT(*) FROM b")

        上下文期间持有数据锁，``apply`` 会把剩余记录一次性应用完；两次查询
        看到的是同一个一致的已应用前缀。**代价**是这段时间副本的 apply 停摆，
        所以只用于确实需要跨语句一致性的短读，不要包住慢操作。
        """
        self._pending_reads += 1
        with self._lock:
            # 在持锁状态下登记，保证与 apply 的组边界互斥：一旦置位，
            # 后续 apply 一定会看到它。
            self._snapshot_depth += 1
            try:
                yield self
            finally:
                self._snapshot_depth -= 1
                self._pending_reads -= 1

    def execute_one(self, sql: str, params=None):
        # 先用无锁计数登记"有读者在等"，apply 的组间让出据此决定是否
        # 减速；计数只影响调度启发式，不承担任何正确性职责。
        self._pending_reads += 1
        try:
            with self._lock:
                return self.db.execute_one(sql, params)
        finally:
            self._pending_reads -= 1

    def close(self) -> None:
        """停止后台线程并关闭数据库句柄。"""
        self._stop_background()
        self._release_stream_client()
        self._adopt_local_lsn()
        self.db.close()

    def _release_stream_client(self) -> None:
        client = self._stream_client
        self._stream_client = None
        self._stream_key = None
        close = getattr(client, "close", None)
        if close is not None:
            try:
                close()
            except Exception:  # noqa: BLE001 - 关闭长连接不能影响收工
                pass

    def add_step_down_listener(self, callback: Callable[[str], None]) -> None:
        """注册降级观察者（``ReplicationMonitor`` 用它把状态改成 needs_attention）。"""
        self._step_down_listeners.append(callback)

    def step_down(self, reason: str = "lease lost") -> Database:
        """停心跳、停续租、切回只读，用于租约丢失/观察到更高任期后的自停写。

        该回调可能在 keeper 或心跳线程中被调用；两个 ``stop()`` 都会识别
        当前线程，避免线程 join 自己。``Database.close()`` 在 ``_write_lock``
        内完成，所以会等在途写语句结束，不会把页写到已关闭的句柄上。

        停止顺序是**先心跳、再 keeper**：反过来的话，心跳线程会在降级之后
        继续以 leader 身份发消息，把旧任期推给 peer。
        """
        self._stop_background()
        with self._lock:
            self._adopt_local_lsn()
            self.db.close()
            self.db = Database(
                self.path,
                read_only=True,
                checkpoint_wal_bytes=0,
                wal_segment_bytes=getattr(self, "_wal_segment_bytes", 1024 * 1024),
            )
            db = self.db
        for callback in list(self._step_down_listeners):
            try:
                callback(reason)
            except Exception:  # noqa: BLE001 - 观察者不能影响降级本身
                _log.exception("step_down 观察者抛错：%r", callback)
        return db

    def _stop_background(self) -> None:
        """先停心跳、再停续租。"""
        heartbeat = self._heartbeat
        self._heartbeat = None
        if heartbeat is not None:
            heartbeat.stop()
        keeper = self._lease_keeper
        self._lease_keeper = None
        if keeper is not None:
            keeper.stop()

    def rebase_from_server(
        self,
        host: str,
        port: int,
        *,
        timeout: float = 10.0,
        token: str | None = None,
        compress: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
    ) -> int:
        """用主库的页级增量备份重建数据文件，并返回新的基础 LSN。

        这用于副本落后于已回收 WAL、但仍持有旧基准备份的场景：主库只发送
        pageLSN 大于本地 ``applied_lsn`` 的页，副本关掉只读句柄、逐页写回
        数据文件、重新打开，再从新的 checkpoint 继续追 WAL。比全量基准备份
        少传大量未变化页；如果本地文件损坏，仍应重新 bootstrap。
        """
        with self._lock:
            from .wal_stream import WALClient

            self.db.close()
            temp = self.path / "incremental-pages.tmp"
            if temp.exists():
                for path in temp.glob("*"):
                    try:
                        path.unlink()
                    except OSError:
                        pass
                try:
                    temp.rmdir()
                except OSError:
                    pass
            try:
                client = WALClient(
                    host, port, timeout=timeout, token=token, compress=compress,
                    ssl_context=ssl_context, server_hostname=server_hostname,
                    replica_id=replica_id,
                )
                checkpoint_lsn, _count = client.incremental_to(
                    temp, self._applied_lsn
                )
                if checkpoint_lsn < self._applied_lsn:
                    raise StorageError(
                        "主库 checkpoint 落后于副本 applied_lsn",
                        detail=(
                            f"checkpoint={checkpoint_lsn} "
                            f"applied={self._applied_lsn}"
                        ),
                    )
                data_path = self.path / "lumendb.data"
                staged_path = self.path / "lumendb.data.rebase.tmp"
                shutil.copyfile(data_path, staged_path)
                disk = DiskManager(staged_path)
                try:
                    for path in sorted(temp.glob("*.page")):
                        page_id = int(path.stem)
                        disk.write_page_bytes(page_id, path.read_bytes())
                    disk.sync()
                finally:
                    disk.close()
                atomic_replace(staged_path, data_path)
                self.db = Database(
                    self.path, read_only=True, checkpoint_wal_bytes=0
                )
                self._applied_lsn = checkpoint_lsn
                self._next_lsn = checkpoint_lsn + 1
                # 基准备份换了数据文件，旧进程内维护的“下一条记录边界”
                # 已经不再对应新的 WAL 位置。
                self._expected_end_lsn = 0
                self._write_marker()
                if replica_id is not None:
                    client.ack(checkpoint_lsn)
                return checkpoint_lsn
            except BaseException:
                # 网络或页传输失败时，副本句柄不能留在已关闭状态。
                if getattr(self.db, "disk", None) is None or self.db.disk._fh.closed:
                    self.db = Database(
                        self.path, read_only=True, checkpoint_wal_bytes=0
                    )
                raise
            finally:
                for path in list(temp.glob("*")):
                    try:
                        path.unlink()
                    except OSError:
                        pass
                try:
                    temp.rmdir()
                except OSError:
                    pass

    def rebuild_from_scratch(
        self,
        host: str,
        port: int,
        *,
        timeout: float = 10.0,
        token: str | None = None,
        compress: bool = False,
        binary_records: bool = False,
        ssl_context: ssl.SSLContext | None = None,
        server_hostname: str | None = None,
        replica_id: str | None = None,
    ) -> int:
        """全量重新基准备份——``rebase_from_server`` 失败后的最后手段。

        页级增量重建在两种情况下会失败：本地数据文件已损坏（页无法安全覆盖），
        或主库已经回收了副本需要的旧页。这时唯一能自愈的做法是重新拉一份完整
        基准备份。**旧数据文件不删除**，改名为 ``broken-<时间戳>/lumendb.data``
        留作取证；调用方拿到返回值后应重置仲裁进度（``reset_progress``），
        否则 leader 下一轮会按旧位置推送、永远拼接点不匹配。

        返回新的基础 LSN。
        """
        with self._lock:
            from .wal_stream import WALClient

            self.db.close()
            data_path = self.path / "lumendb.data"
            preserved = ""
            if data_path.exists():
                stamp = time.strftime("%Y%m%d-%H%M%S")
                broken = self.path / f"broken-{stamp}"
                broken.mkdir(parents=True, exist_ok=True)
                data_path.rename(broken / "lumendb.data")
                preserved = str(broken)
                _log.warning("旧数据文件已保留到 %s，开始全量重建", preserved)
            client = WALClient(
                host, port, timeout=timeout, token=token, compress=compress,
                binary_records=binary_records,
                ssl_context=ssl_context, server_hostname=server_hostname,
                replica_id=replica_id,
            )
            base_lsn = client.backup_to(data_path)
            try:
                self.db = Database(
                    self.path, read_only=True, checkpoint_wal_bytes=0
                )
            except Exception:
                # 基准备份文件打不开（磁盘截断/校验失败）时退成可写句柄，
                # 至少让副本仍能对外提供句柄；下一次重建会再次覆盖这份文件。
                _log.warning("重建后的数据文件无法以只读方式打开，回退普通句柄")
                self.db = Database(self.path, checkpoint_wal_bytes=0)
            self._applied_lsn = base_lsn
            self._next_lsn = base_lsn + 1
            self._expected_end_lsn = 0
            self._write_marker()
            self.last_rebuild_preserved = preserved
            if replica_id is not None:
                client.ack(base_lsn)
            return base_lsn

    def promote_with_quorum(
        self,
        quorum,
        *,
        fence_path: str | Path | None = None,
        lease_path: str | Path | None = None,
        lease_ttl: float = 10.0,
        lease_keeper_interval: float | None = None,
        lease_keeper_auto: bool = True,
        lease_renew_with_quorum: bool = False,
        lease_loss_callback: Callable[[LeaseKeeper], None] | None = None,
        lease_loss_stepdown: bool = False,
        heartbeat_interval: float | None = None,
        push_max_batches_per_round: int = 10,
        on_higher_term: Callable[[int], None] | None = None,
        sync_replication: bool = False,
        sync_timeout: float = 2.0,
        node_id: str | None = None,
    ) -> Database:
        """先拿到多数派投票，再执行 :meth:`promote`。

        ``quorum`` 是 :class:`lumendb.replication.quorum.QuorumNode`；投票失败会
        直接抛 ``NotSupportedError``，副本保持只读。投票成功后才切换读写，
        若给了 ``fence_path``，旧主库仍能通过 fence 拒绝写入；若给了
        ``lease_path``，新主库会持有写租约，并把 ``LeaseFence``（租约 + 当前
        任期）装到新主库的写路径上，租约失效或观察到更高任期后主库自行拒绝
        写入。

        后台线程（``close()`` / ``step_down()`` 会先停心跳、再停 keeper）：

        * **续租**：``lease_keeper_interval`` 指定周期；不指定时按
          ``lease_keeper_auto``（默认开）用 ``ttl / 3`` 自动启动。给了
          ``lease_path`` 却不续租，新主库会在 TTL 到点后自行停写——这是有意
          的护栏，但通常不是调用方想要的，所以默认替你起 keeper。
          ``lease_renew_with_quorum=True`` 时续租前必须先拿到多数派心跳。
        * **心跳**：``heartbeat_interval`` 非空时启动 :class:`HeartbeatLoop`，
          维持 leader 活跃度并观察任期。观察到更高任期时调用
          ``on_higher_term(term)``；未提供时若 ``lease_loss_stepdown`` 为真就
          直接 ``step_down()``，否则只记录在 ``last_heartbeat_errors``。
        * **追赶推送**：每轮心跳后自动把本地 WAL 推给 peers，一轮最多
          ``push_max_batches_per_round`` 批（每批上限 1000 条），追平即停。
        """
        if sync_replication and heartbeat_interval is None:
            # 同步复制靠心跳轮次推进多数派水位；没有心跳就等于每笔写都要
            # 等到超时。宁可在提升前明确拒绝。
            raise NotSupportedError(
                "同步复制需要心跳循环推进多数派水位",
                detail="sync_replication=True 但 heartbeat_interval 为空",
                hint="传 heartbeat_interval（例如 0.5），或关掉 sync_replication",
            )
        candidate_node = node_id or quorum.node_id
        self._stop_background()
        # 先把自己的复制进度登记到仲裁节点，再投票：水位计算和提升闸门都
        # 需要知道"候选到底持有到哪"。
        self.attach_quorum(quorum)
        quorum.campaign(
            self._applied_lsn,
            fence_path=fence_path,
            lease_path=lease_path,
            lease_ttl=lease_ttl,
        )
        lease = (
            Lease(lease_path, candidate_node, ttl=lease_ttl)
            if lease_path is not None
            else None
        )
        term_of = lambda: getattr(getattr(quorum, "state", None), "term", None)
        fence = LeaseFence(lease, term_of) if lease is not None else None
        if lease is not None:
            # 提升后的新主库要以持有者身份打开数据库（Database 打开时的
            # 租约重启防护会校验持有者）；轻量 mock quorum 的 campaign 不会
            # acquire，这里兜底保证租约一定落在自己名下。acquire 对自己
            # 幂等（等价续租）。任期优先取 quorum 的当前任期；拿不到时
            # （mock 没有 state）沿用租约文件里已有的任期，避免触发
            # "任期回退"拒绝。
            acquire_term = term_of()
            if acquire_term is None:
                existing = lease.read()
                acquire_term = int(existing["term"]) if existing else 0
            lease.acquire(int(acquire_term))
        db = self.promote(
            fence_path=fence_path,
            lease_path=lease_path,
            write_fence=fence.assert_writable if fence is not None else None,
            node_id=candidate_node,
        )

        # 记录"新任期从哪个 LSN 开始写"：之后 leader 推送这批记录时，
        # 每条都能报出精确任期，follower 才能在拼接点发现历史分叉。
        # quorum 允许是轻量 mock（没有 state），这里按 duck-typing 兜底。
        next_lsn = int(getattr(self, "_next_lsn", 0))
        if next_lsn <= 0:
            next_lsn = int(getattr(db.wal, "next_lsn", 0))
        state = getattr(quorum, "state", None)
        if next_lsn > 0 and state is not None and hasattr(state, "record_term"):
            state.record_term(next_lsn, int(getattr(state, "term", 0)))

        if lease is not None:
            interval = lease_keeper_interval
            if interval is None and lease_keeper_auto:
                interval = max(0.05, lease_ttl / 3)
            if interval is None:
                warnings.warn(
                    "给了 lease_path 但没有启动续租线程：主库会在租约 TTL 到期后"
                    "自行停写。传 lease_keeper_interval，或确认这是你要的护栏。",
                    RuntimeWarning,
                    stacklevel=2,
                )
            else:
                self._lease_keeper = self._make_lease_keeper(
                    lease=lease,
                    interval=interval,
                    quorum=quorum,
                    lease_path=lease_path,
                    lease_ttl=lease_ttl,
                    lease_renew_with_quorum=lease_renew_with_quorum,
                    lease_loss_callback=lease_loss_callback,
                    lease_loss_stepdown=lease_loss_stepdown,
                    term_of=term_of,
                ).start()

        if heartbeat_interval is not None:
            callback = on_higher_term
            if callback is None and lease_loss_stepdown:
                callback = lambda term: self.step_down(f"observed higher term {term}")
            self._heartbeat = HeartbeatLoop(
                quorum,
                lambda: self._current_lsn(),
                interval=heartbeat_interval,
                on_higher_term=callback,
                push_entries=self.push_entries_to_peers,
                max_batches_per_round=push_max_batches_per_round,
            ).start()

        if sync_replication:
            # 装上提交屏障：本地 COMMIT fsync 之后还要等多数派水位追平，
            # 才把成功报给客户端。超时抛 40003（结果未知），不回滚。
            self._commit_barrier = QuorumCommitBarrier(quorum, timeout=sync_timeout)
            db.set_commit_barrier(self._commit_barrier)
        else:
            self._commit_barrier = None
        return db

    def _make_lease_keeper(
        self,
        *,
        lease: Lease,
        interval: float,
        quorum,
        lease_path,
        lease_ttl: float,
        lease_renew_with_quorum: bool,
        lease_loss_callback: Callable[[LeaseKeeper], None] | None,
        lease_loss_stepdown: bool,
        term_of,
    ) -> LeaseKeeper:
        def renew():
            return lease.renew(term=term_of())

        if lease_renew_with_quorum:
            def renew():
                return quorum.renew_lease(
                    self._current_lsn(),
                    lease_path=lease_path,
                    lease_ttl=lease_ttl,
                )

        def on_loss(keeper):
            try:
                if lease_loss_callback is not None:
                    lease_loss_callback(keeper)
            finally:
                if lease_loss_stepdown:
                    self.step_down(f"lease lost: {keeper.last_error}")

        failure_handler = (
            on_loss
            if lease_loss_callback is not None or lease_loss_stepdown
            else None
        )
        return LeaseKeeper(
            lease,
            interval=interval,
            renew=renew,
            on_failure=failure_handler,
        )

    def commit_barrier(self) -> QuorumCommitBarrier | None:
        """当前的提交屏障（`sync_replication=True` 时非空）。"""
        return self._commit_barrier

    @property
    def last_heartbeat_errors(self) -> list[str]:
        """最近一轮心跳的错误（没有心跳线程时为空）。"""
        heartbeat = self._heartbeat
        return list(heartbeat.last_errors) if heartbeat is not None else []

    def promote(
        self,
        *,
        fence_path: str | Path | None = None,
        lease_path: str | Path | None = None,
        node_id: str = "replica",
        write_fence: Callable[[], None] | None = None,
    ) -> Database:
        """关闭只读句柄并以读写模式重新打开，完成手动提升。

        若给出 ``fence_path``，先原子写入提升 marker；旧主库如果配置了同一
        路径的 ``PromotionFence.assert_writable`` 写回调，就会在下一笔写语句
        上拒绝，形成最小的防双写约束。``write_fence`` 可把 lease/guard 装到
        新主库的写路径上。没有自动选举和网络租约。
        """
        # 提升会替换数据库句柄：旧句柄上的心跳/续租线程必须先停，
        # 否则它们会继续用旧租约/旧任期对外发消息。
        self._stop_background()
        with self._lock:
            if fence_path is not None:
                fence = PromotionFence(fence_path, node_id=node_id)
                fence.promote(self._applied_lsn)
                # 没有显式 write_fence（租约等）时，默认把共享 fence 的
                # assert_writable 接进写路径：每个主库写前都会重读 marker，
                # 别的节点后来提升会立即让旧主库停写，而不是只有提升瞬间
                # 写一次 marker 就再也不看。
                if write_fence is None:
                    write_fence = fence.assert_writable
            self.db.close()
            self.db = Database(
                self.path,
                read_only=False,
                checkpoint_wal_bytes=0,
                wal_segment_bytes=getattr(self, "_wal_segment_bytes", 1024 * 1024),
                write_fence=write_fence,
                fence_path=fence_path,
                lease_path=lease_path,
                node_id=node_id,
            )
            self._seed_promoted_wal()
            self._adopt_local_lsn()
            return self.db

    def _seed_promoted_wal(self) -> None:
        """把新主库的空 WAL 接到副本已复制到的边界。

        基准备份和复制 apply 都不在副本上写 WAL，所以提升后打开的数据库
        默认从 LSN 1 开始追加；旧副本已经 apply 到更高的 LSN，会把新主库
        的第一批记录当成“落后/重叠”。这里在 WAL 仍为空时，把 next_lsn
        设到上一批记录结束处，保持整个集群的 LSN 单调。
        """
        wal = getattr(self.db, "wal", None)
        if wal is None or int(getattr(wal, "last_lsn", 0)) != 0:
            return
        seed = getattr(wal, "seed_next_lsn", None)
        if seed is None:
            # 空 WAL 又不能接边界（旧单文件 WAL 没有 seed 能力）：如果放行，
            # 新主库会从 LSN 1 开始写，与已复制到高位的记录冲突，副本会
            # 静默拒绝后续记录。宁可在这里失败，也不静默制造 LSN 回退。
            raise NotSupportedError(
                "提升后的空 WAL 无法接续复制边界",
                hint="需要分段 WAL（wal_segment_bytes）的 seed_next_lsn 能力",
            )
        next_lsn = max(1, int(self._next_lsn))
        if next_lsn <= 1:
            return
        seed(next_lsn, last_lsn=int(self._applied_lsn))
        self._next_lsn = next_lsn


__all__ = ["Replica"]
