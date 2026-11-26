"""后台任务骨架：统一的注册、启动顺序、**反向停止**与错误上报。

## 为什么需要它

这个项目里有 9 个后台线程（租约续期、leader 心跳、选举看门狗、副本重建、
监控循环、WALServer 的 accept/handler……），它们各自管自己的
`start()/stop()/daemon`。已经因此付过四次学费：

* 停心跳与停 keeper 顺序反了 → 被废黜的主库继续以 leader 身份发心跳；
* keeper 回调线程里 stop 自己 → 自 join 死锁；
* WALServer handler 线程不回收 → 线程泄漏；
* 后台重建线程与提升并发 → 已提升主库的数据被重建清掉。

这些都不是"某一行写错了"，而是**没有骨架**：谁先停、谁等谁、出错报给谁，
全靠每个类自己记得。

## 契约

* **启动按注册顺序，停止按相反顺序**——依赖方先停；
* **停止幂等**：重复 `stop_all()` 安全，未启动的不停；
* **谁的线程谁负责 join**：注册表不直接 join，交给各个 `stop()`（它们已经知道
  自己是不是当前线程，能避免自 join）；
* **一个失败不拖垮其它**：停止阶段的异常被记录到 `status()`，不会中断后续停止；
* **启动失败要回滚**：已经起来的先停掉，再把异常抛给调用方。

## 迁移状态与豁免（2026-11）

已迁：`Replica` 的 keeper / heartbeat（`_stop_background()` → `registry.clear()`）；
`ReplicationMonitor` 的 poll / election-watchdog（内部注册表，`start_all()` 按序启动，
`stop_all()` 反向停止，`services_status()` 可观测）。

豁免（**不**归注册表管，理由写在代码里）：

* `wal_stream` 的 accept / per-connection handler 线程——生命周期是"连接"而不是
  "进程"，靠 `WALServer.stop()` + handler prune 兜底；
* `replica._schedule_rebuild` 的临时线程——生命周期是"一次重建"，靠
  `_rebuild_in_flight` 去重；
* 测试里的假对象。

新增后台任务时：只要能"启动一次、活很久、随 close 停"，就必须走注册表；
`tests/unit/test_thread_leaks.py` 会在创建/关闭之后断言"没有 `lumendb-*`
线程残留"，漏停会红。

用法：

```python
services = ServiceRegistry()
services.register("lease-keeper", keeper)
services.register("heartbeat", loop)
services.start_all()
...
services.stop_all()          # 反向：先停 heartbeat，再停 keeper
assert services.healthy
```
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Optional


@dataclass
class Service:
    """一个可启动/可停止的后台任务。

    ``target`` 只要有 ``start()`` 与 ``stop()`` 即可（`LeaseKeeper`、
    `HeartbeatLoop`、`ReplicationMonitor` 都满足）；也可以直接给两个可调用对象。
    """

    name: str
    start: Callable[[], Any]
    stop: Callable[[], Any]
    started: bool = False
    last_error: Optional[str] = None
    extra: dict[str, Any] = field(default_factory=dict)

    def snapshot(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "started": self.started,
            "last_error": self.last_error,
            **self.extra,
        }


class ServiceRegistry:
    """注册表：负责顺序、幂等、错误上报。不负责线程细节。"""

    def __init__(self, name: str = "services") -> None:
        self.name = name
        self._services: list[Service] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        target: Any = None,
        *,
        start: Optional[Callable[[], Any]] = None,
        stop: Optional[Callable[[], Any]] = None,
    ) -> Service:
        """登记一个任务。``target`` 需带 ``start()/stop()``，或显式给两个回调。"""
        if target is not None:
            start = start or target.start
            stop = stop or target.stop
        if start is None or stop is None:
            raise ValueError(f"服务 {name!r} 需要 start/stop（或带这两个方法的 target）")
        if name in {s.name for s in self._services}:
            raise ValueError(f"服务 {name!r} 重复注册")
        service = Service(name=name, start=start, stop=stop)
        with self._lock:
            self._services.append(service)
        return service

    # ------------------------------------------------------------------

    def start_all(self) -> None:
        """按注册顺序启动；任一失败则回滚已启动的，再抛出。"""
        started: list[Service] = []
        try:
            for service in list(self._services):
                if service.started:
                    started.append(service)
                    continue
                try:
                    service.start()
                except BaseException as exc:  # noqa: BLE001 - 记录后回滚
                    service.last_error = f"start: {exc}"
                    raise
                service.started = True
                started.append(service)
        except BaseException:
            for service in reversed(started):
                self._stop_one(service)
            raise

    def stop_all(self) -> None:
        """按**相反**顺序停止；幂等；单个失败不影响其它。"""
        for service in reversed(list(self._services)):
            if not service.started:
                continue
            self._stop_one(service)

    def _stop_one(self, service: Service) -> None:
        try:
            service.stop()
            service.last_error = None
        except BaseException as exc:  # noqa: BLE001 - 停止阶段不能中断后续
            service.last_error = f"stop: {exc}"
        finally:
            service.started = False

    # ------------------------------------------------------------------

    @property
    def healthy(self) -> bool:
        return all(s.last_error is None for s in self._services)

    @property
    def running(self) -> list[str]:
        return [s.name for s in self._services if s.started]

    def status(self) -> dict[str, Any]:
        with self._lock:
            services = [s.snapshot() for s in self._services]
        return {
            "name": self.name,
            "healthy": all(s["last_error"] is None for s in services),
            "running": [s["name"] for s in services if s["started"]],
            "services": services,
        }

    def clear(self) -> None:
        """停掉全部并清空登记（用于"提升/降级重建后台线程"的场景）。"""
        self.stop_all()
        with self._lock:
            self._services.clear()

    def get(self, name: str) -> Service:
        for service in self._services:
            if service.name == name:
                return service
        raise KeyError(name)

    def __len__(self) -> int:
        return len(self._services)

    def __enter__(self) -> "ServiceRegistry":
        self.start_all()
        return self

    def __exit__(self, *exc) -> None:
        self.stop_all()


def register_all(registry: ServiceRegistry, items: Iterable[tuple[str, Any]]) -> None:
    """批量登记：``register_all(r, [("keeper", keeper), ("heartbeat", loop)])``。"""
    for name, target in items:
        registry.register(name, target)


__all__ = ["Service", "ServiceRegistry", "register_all"]
