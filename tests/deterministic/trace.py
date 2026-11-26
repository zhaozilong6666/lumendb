"""确定性重放的因果轨迹与一次性闸门。

这里不试图替代线程调度器，而是先把**真实引擎里最关键的交错点**变成可命名、
可等待、可断言顺序的闸门：

* ``Trace.mark()`` 在事件发生时记下 ``(序号, 线程名, 事件名, 字段)``；
* ``HoldPoint.wait()`` 让第一个到达的线程停住，直到测试显式 ``release``；
* ``Trace.wait_for()`` 让主测试线程等到某个事件真的发生，而不是 ``sleep`` 猜。

固定种子只负责产生同一组参数（键值、插入数量、闸门位置）；真正的交错顺序
由这些闸门和断言固定。失败时 ``Trace.dump()`` 会打印完整事件序，可直接按
``seed`` 重跑。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Event:
    seq: int
    at: float
    thread: str
    name: str
    fields: dict[str, Any] = field(default_factory=dict)


class Trace:
    """线程安全、可等待、可打印的事件轨迹。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._events: list[Event] = []

    def mark(self, name: str, **fields: Any) -> Event:
        thread = threading.current_thread().name
        with self._cond:
            event = Event(
                seq=len(self._events),
                at=time.perf_counter(),
                thread=thread,
                name=name,
                fields=dict(fields),
            )
            self._events.append(event)
            self._cond.notify_all()
            return event

    def wait_for(
        self, name: str, *, thread: Optional[str] = None, timeout: float = 5.0
    ) -> Event:
        """等到某个事件出现；超时直接失败并打印已有轨迹。"""
        deadline = time.monotonic() + timeout
        with self._cond:
            while True:
                for event in self._events:
                    if event.name == name and (
                        thread is None or event.thread == thread
                    ):
                        return event
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError(
                        f"等待事件 {name!r}（thread={thread!r}）超时；"
                        f"已有轨迹：\n{self.dump()}"
                    )
                self._cond.wait(remaining)

    def events(
        self, name: Optional[str] = None, *, thread: Optional[str] = None
    ) -> list[Event]:
        with self._lock:
            return [
                e
                for e in self._events
                if (name is None or e.name == name)
                and (thread is None or e.thread == thread)
            ]

    def count(self, name: str, *, thread: Optional[str] = None) -> int:
        return len(self.events(name, thread=thread))

    def first_seq(self, name: str, *, thread: Optional[str] = None) -> int:
        events = self.events(name, thread=thread)
        if not events:
            raise AssertionError(
                f"事件 {name!r}（thread={thread!r}）没有发生；\n{self.dump()}"
            )
        return events[0].seq

    def assert_before(
        self, before: str, after: str, *, thread: Optional[str] = None
    ) -> None:
        b = self.first_seq(before, thread=thread)
        a = self.first_seq(after, thread=thread)
        if b >= a:
            raise AssertionError(
                f"因果顺序错误：{before!r} 应在 {after!r} 之前；\n{self.dump()}"
            )

    def dump(self) -> str:
        with self._lock:
            lines = ["seq | thread | event | fields", "----+--------+-------+-------"]
            for e in self._events:
                fields = " ".join(f"{k}={v!r}" for k, v in e.fields.items())
                lines.append(
                    f"{e.seq:3d} | {e.thread:20s} | {e.name:24s} | {fields}"
                )
            return "\n".join(lines)


class HoldPoint:
    """一次性闸门：第一个 ``wait()`` 的线程停住，直到 ``release()``。"""

    def __init__(self, name: str, trace: Trace, *, timeout: float = 10.0) -> None:
        self.name = name
        self.trace = trace
        self._entered = threading.Event()
        self._release = threading.Event()
        self._timeout = timeout

    @property
    def entered(self) -> threading.Event:
        return self._entered

    def wait(self) -> None:
        if self._entered.is_set():
            # 第二个线程不该被同一个闸门拦住；它只记录事件后继续。
            self.trace.mark(f"{self.name}.pass", thread=threading.current_thread().name)
            return
        self._entered.set()
        self.trace.mark(f"{self.name}.wait")
        if not self._release.wait(self._timeout):
            raise AssertionError(
                f"闸门 {self.name!r} 等待 {self._timeout}s 未释放；\n"
                f"{self.trace.dump()}"
            )
        self.trace.mark(f"{self.name}.release")

    def release(self) -> None:
        self._release.set()
