"""第一个确定性重放场景：叶分裂 → 父键装填的结构窗口。

固定种子只决定：分裂键选哪一个、 follower 从哪个键开始、插多少。真正的
交错由 ``HoldPoint`` 固定在“A 已分裂、父键还没装”的那一刻，然后让 follower
去填满新右页。没有 ``_split_lock`` 的旧实现会在这里直接把新右页再分裂一次，
随后找不到父页并留下半截分裂；新实现会让 follower 停在结构锁上，等 A 装完
父键再继续。
"""

from __future__ import annotations

import pathlib
import threading
import time
from dataclasses import dataclass

from lumendb.datatypes import DataType, TypeSpec
from lumendb.engine.database import Database
from lumendb.storage.btree import BPlusTree, KeyCodec

from .trace import HoldPoint, Trace


@dataclass(frozen=True)
class Case:
    seed: int
    split_key: int
    follower_start: int
    follower_count: int


def make_case(seed: int) -> Case:
    if seed < 0:
        raise ValueError("seed 不能为负")
    return Case(
        seed=seed,
        split_key=6 + seed % 4,
        follower_start=20 + seed % 3,
        follower_count=15 + seed % 5,
    )


def _long_key(i: int) -> str:
    return "k" + "x" * 400 + f"{i:05d}"


def run_case(seed: int, tmp_path: pathlib.Path) -> tuple[Trace, dict]:
    """跑一个固定种子场景；返回轨迹与不变量统计。"""
    case = make_case(seed)
    db = Database(tmp_path, checkpoint_wal_bytes=0)
    tree = BPlusTree(
        db.bp, KeyCodec([TypeSpec(DataType.VARCHAR, 512)]), unique=False, name="idx"
    )
    trace = Trace()
    hold = HoldPoint(f"seed{seed}.parent_install", trace)
    errors: list[str] = []
    first_install = threading.Lock()
    install_seen = [False]

    # 9 条长键刚好占满一片叶；第 10 条（不同 RID 的同值键）触发根叶分裂。
    t0 = db.txns.begin()
    for i in range(9):
        tree.insert(t0, (_long_key(i),), (0, i))
    db.txns.commit(t0)

    original_split = tree._split_leaf
    original_insert_at = tree._leaf_insert_at
    original_install = tree._install_sep

    def traced_split(txn, page, keys, rids):
        trace.mark("split.start", page=page.page_id, n=len(keys))
        out = original_split(txn, page, keys, rids)
        trace.mark("split.end", page=page.page_id, sep=out[0])
        return out

    def traced_insert_at(txn, page, key, rid):
        idx = original_insert_at(txn, page, key, rid)
        if idx is not None:
            trace.mark("needs_split", page=page.page_id, key=key)
        return idx

    def traced_install(txn, left_id, sep, right_id):
        should_hold = False
        with first_install:
            if not install_seen[0]:
                install_seen[0] = True
                should_hold = True
        trace.mark("install.start", left=left_id, right=right_id)
        try:
            if should_hold:
                hold.wait()
            return original_install(txn, left_id, sep, right_id)
        finally:
            trace.mark("install.end", left=left_id, right=right_id)

    tree._split_leaf = traced_split  # type: ignore[method-assign]
    tree._leaf_insert_at = traced_insert_at  # type: ignore[method-assign]
    tree._install_sep = traced_install  # type: ignore[method-assign]

    def splitter() -> None:
        try:
            txn = db.txns.begin()
            tree.insert(txn, (_long_key(case.split_key),), (1, case.split_key))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"splitter: {type(exc).__name__}: {exc}")

    def follower() -> None:
        try:
            txn = db.txns.begin()
            for i in range(
                case.follower_start, case.follower_start + case.follower_count
            ):
                tree.insert(txn, (_long_key(i),), (2, i))
            db.txns.commit(txn)
        except BaseException as exc:  # noqa: BLE001
            errors.append(f"follower: {type(exc).__name__}: {exc}")

    first = threading.Thread(target=splitter, name="splitter")
    second = threading.Thread(target=follower, name="follower")
    try:
        first.start()
        assert hold.entered.wait(timeout=5), f"splitter 没有到达父键装填窗口；\n{trace.dump()}"
        second.start()
        # follower 必须真的走到“页已满、准备分裂”那一步，不能用 sleep 猜。
        trace.wait_for("needs_split", thread="follower", timeout=5)
        # 新实现应让它停在 _split_lock；旧实现会在这里已经 split.start。
        time.sleep(0.05)
        if trace.count("split.start", thread="follower") != 0:
            raise AssertionError(
                "follower 在 splitter 装父键之前开始了第二次分裂；\n"
                f"{trace.dump()}"
            )

        hold.release()
        first.join(timeout=30)
        second.join(timeout=30)
        assert not first.is_alive() and not second.is_alive(), "线程未退出"
        assert errors == [], f"确定性场景出错: {errors[:2]}；\n{trace.dump()}"

        follower_split = trace.events("split.start", thread="follower")
        assert follower_split, f"follower 没有触发第二次分裂；\n{trace.dump()}"
        splitter_install_end = trace.events(
            "install.end", thread="splitter"
        )
        assert splitter_install_end, f"splitter 没有完成父键装填；\n{trace.dump()}"
        assert splitter_install_end[0].seq < follower_split[0].seq, (
            "父键装填必须发生在 follower 的第二次分裂之前；\n"
            f"{trace.dump()}"
        )

        stats = tree.check_invariants()
        assert stats["entries"] == 9 + 1 + case.follower_count
        return trace, {"stats": stats, "case": case}
    finally:
        hold.release()
        try:
            db.close()
        except BaseException:
            pass
