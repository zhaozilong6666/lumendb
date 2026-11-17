"""同一个 Database 上的并发语句：当前只保证"正确"，不保证"并行"。

Executor 把每条语句的状态（binder / `$n` 参数 / CTE 作用域 / 递归工作表）
存在实例字段上，因此同一个 Database 不可能同时跑两条语句。读语句不走
`_write_lock`（备份期间要能继续读），所以这条不变量必须由 `_statement_lock`
单独保证——否则两个线程同时 SELECT 会互相覆盖 binder，拿到对方的参数，
或者丢掉 CTE 作用域。

这里既测"不并发时结果正确"，也测"并发了仍然正确"（不会互相踩 binder），
但**不**主张它们并行执行：真正的并发语句要把 Executor 的语句状态改成按调用
传递的上下文（见 docs/concurrency.md）。
"""

from __future__ import annotations

import threading

import pytest

from lumendb.engine.database import Database


@pytest.fixture
def db(tmp_path):
    db = Database(tmp_path / "db", checkpoint_wal_bytes=0)
    db.execute_one("CREATE TABLE t (a INT PRIMARY KEY)")
    for value in (1, 2, 3):
        db.execute_one(f"INSERT INTO t VALUES ({value})")
    yield db
    db.close()


def _run_threads(target, count: int) -> list:
    results: list = []
    errors: list = []

    def wrapper(index: int) -> None:
        try:
            results.append(target(index))
        except BaseException as exc:  # noqa: BLE001 - 测试里要把异常带回来
            errors.append(exc)

    threads = [
        threading.Thread(target=wrapper, args=(i,)) for i in range(count)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    if errors:
        raise errors[0]
    return results


def test_concurrent_cte_and_plain_selects_do_not_clobber_the_binder(db):
    """两个线程同时跑"带 CTE 的查询"和"普通查询"，结果都必须正确。

    CTE 作用域存在 `executor._active_ctes`，普通查询会把它设成空字典；
    没有语句锁时，带 CTE 的那条会在执行中丢掉自己的作用域，报
    "CTE not found" 或读到错的表。
    """

    def query(index: int) -> list:
        if index % 2 == 0:
            return db.execute_one(
                "WITH c AS (SELECT a FROM t WHERE a > 1) SELECT COUNT(*) FROM c"
            ).rows
        return db.execute_one("SELECT COUNT(*) FROM t").rows

    results = _run_threads(query, 8)
    assert results.count([(2,)]) == 4, results  # CTE 分支
    assert results.count([(3,)]) == 4, results  # 普通分支


def test_concurrent_selects_with_parameters_stay_isolated(db):
    """`$n` 参数存在 `executor._params` 上：并发时必须各用各的值。"""

    def query(index: int) -> int:
        return db.execute_one("SELECT a FROM t WHERE a = $1", [index % 3 + 1]).rows[0][0]

    results = _run_threads(query, 9)
    assert sorted(results) == [1, 1, 1, 2, 2, 2, 3, 3, 3]


def test_writes_and_reads_from_threads_are_serialized_not_corrupted(db):
    """读线程 + 写线程混跑：总数必须始终自洽（不出现半条语句的中间态）。"""

    def work(index: int) -> int:
        if index % 3 == 0:
            db.execute_one(f"INSERT INTO t VALUES ({100 + index})")
        return db.execute_one("SELECT COUNT(*) FROM t").rows[0][0]

    _run_threads(work, 9)
    total = db.execute_one("SELECT COUNT(*) FROM t").rows[0][0]
    assert total == 3 + 3  # 9 个任务里 index 0/3/6 各插一行
