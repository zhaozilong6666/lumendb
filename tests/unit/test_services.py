"""后台任务骨架自身的契约测试。

这些用例不测具体服务，只测注册表承诺的四件事：**顺序、幂等、错误隔离、失败回滚**。
它们对应的是这个项目已经踩过的四类 bug（见 `lumendb/services.py` 顶部注释）。
"""

from __future__ import annotations

import pytest

from lumendb.services import ServiceRegistry, register_all


class _Recorder:
    """记录 start/stop 调用顺序的假服务。"""

    def __init__(self, name: str, log: list[str], *, fail_start: bool = False,
                 fail_stop: bool = False) -> None:
        self.name = name
        self.log = log
        self.fail_start = fail_start
        self.fail_stop = fail_stop
        self.stop_calls = 0

    def start(self) -> None:
        if self.fail_start:
            raise RuntimeError(f"{self.name} start boom")
        self.log.append(f"start:{self.name}")

    def stop(self) -> None:
        self.stop_calls += 1
        if self.fail_stop:
            raise RuntimeError(f"{self.name} stop boom")
        self.log.append(f"stop:{self.name}")


def test_starts_in_order_and_stops_in_reverse():
    log: list[str] = []
    keeper, heartbeat = _Recorder("keeper", log), _Recorder("heartbeat", log)
    registry = ServiceRegistry()
    register_all(registry, [("keeper", keeper), ("heartbeat", heartbeat)])

    registry.start_all()
    assert log == ["start:keeper", "start:heartbeat"]
    assert registry.running == ["keeper", "heartbeat"]

    registry.stop_all()
    # 反向：先停心跳，再停 keeper（顺序反了会让被废黜的主库继续发心跳）
    assert log == [
        "start:keeper",
        "start:heartbeat",
        "stop:heartbeat",
        "stop:keeper",
    ]
    assert registry.running == []
    assert registry.healthy


def test_stop_is_idempotent_and_skips_unstarted():
    log: list[str] = []
    keeper = _Recorder("keeper", log)
    registry = ServiceRegistry()
    registry.register("keeper", keeper)

    registry.stop_all()  # 从未启动：不应该调用 stop
    assert keeper.stop_calls == 0

    registry.start_all()
    registry.stop_all()
    registry.stop_all()  # 重复停止：不再调用
    assert keeper.stop_calls == 1


def test_one_failure_does_not_block_other_stops():
    log: list[str] = []
    first, second = _Recorder("first", log), _Recorder("second", log, fail_stop=True)
    registry = ServiceRegistry()
    register_all(registry, [("first", first), ("second", second)])
    registry.start_all()

    registry.stop_all()  # 反向：second 先停（抛错），first 仍要被停掉
    assert "stop:first" in log
    assert not registry.healthy
    assert "stop boom" in (registry.get("second").last_error or "")


def test_start_failure_rolls_back_already_started():
    log: list[str] = []
    first = _Recorder("first", log)
    broken = _Recorder("broken", log, fail_start=True)
    registry = ServiceRegistry()
    register_all(registry, [("first", first), ("broken", broken)])

    with pytest.raises(RuntimeError):
        registry.start_all()

    assert "stop:first" in log, "启动失败必须把已经起来的服务停掉"
    assert registry.running == []
    assert "start boom" in (registry.get("broken").last_error or "")


def test_start_all_is_idempotent_for_already_started_services():
    log: list[str] = []
    keeper = _Recorder("keeper", log)
    registry = ServiceRegistry()
    registry.register("keeper", keeper)
    registry.start_all()
    registry.start_all()
    assert log == ["start:keeper"]


def test_status_snapshot_is_json_friendly():
    log: list[str] = []
    registry = ServiceRegistry("replica-services")
    registry.register("keeper", _Recorder("keeper", log))
    registry.start_all()
    status = registry.status()
    assert status["name"] == "replica-services"
    assert status["running"] == ["keeper"]
    assert status["healthy"] is True
    assert status["services"][0]["name"] == "keeper"
    registry.stop_all()


def test_register_rejects_duplicates_and_incomplete_targets():
    registry = ServiceRegistry()
    registry.register("a", _Recorder("a", []))
    with pytest.raises(ValueError):
        registry.register("a", _Recorder("a", []))
    with pytest.raises(ValueError):
        registry.register("b")


def test_register_accepts_explicit_callables():
    log: list[str] = []
    registry = ServiceRegistry()
    registry.register("cb", start=lambda: log.append("start"), stop=lambda: log.append("stop"))
    registry.start_all()
    registry.stop_all()
    assert log == ["start", "stop"]


def test_context_manager_stops_on_exit():
    log: list[str] = []
    with ServiceRegistry() as registry:
        registry.register("keeper", _Recorder("keeper", log))
        registry.start_all()
        assert log == ["start:keeper"]
    assert log == ["start:keeper", "stop:keeper"]


def test_stop_all_tolerates_a_service_that_never_started_but_errors():
    """停止阶段的异常不能把整个 close() 打断（否则会泄漏其它线程）。"""
    log: list[str] = []

    class Bad:
        def start(self) -> None:
            log.append("start")

        def stop(self) -> None:
            raise RuntimeError("stop failed")

    registry = ServiceRegistry()
    registry.register("bad", Bad())
    registry.register("good", _Recorder("good", log))
    registry.start_all()
    registry.stop_all()
    assert "stop:good" in log
