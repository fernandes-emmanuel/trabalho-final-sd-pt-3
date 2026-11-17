from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping
from enum import Enum, auto
from typing import Any

from mpcomm.model import Address, TransportStats
from mpcomm.ordering.total_order import Ordering, TotalOrderMulticast
from mpcomm.transport.channel import Channel
from mpcomm.transport.messages import DataPacket, Kind, Packet
from mpcomm.transport.reliable import PeriodicTask, ReliableFifoMulticast, RetransmissionPolicy

log = logging.getLogger(__name__)

RETRANSMISSION_TICK = 0.01

DeliverCallback = Callable[[int, Any], None]


class SessionState(Enum):
    CREATED = auto()
    OPEN = auto()
    CLOSING = auto()
    CLOSED = auto()


class SessionClosed(Exception):
    pass


class GroupSession:
    """Group communication session between a fixed set of members.

    Messages travel over reliable FIFO multicast. With ``Ordering.TOTAL`` they are also
    totally ordered with Lamport clocks, so every member delivers the same messages in the
    same order.

    Opening multicasts HELLO and waits until every member's HELLO is delivered. Closing
    first lets every member announce, in total order, that it will not send anything else
    (when total ordering is on), then multicasts END over FIFO and waits until every
    member's END is delivered and every message sent by this member is acknowledged.

    Locks are always taken in the order issue -> total order -> send -> transport, and the
    receiving thread never holds a lock while handing messages up, so no cycle can form.
    """

    def __init__(
        self,
        *,
        session_id: int,
        self_id: int,
        members: Mapping[int, Address],
        channel: Channel,
        deliver: DeliverCallback,
        ordering: Ordering = Ordering.FIFO,
        policy: RetransmissionPolicy | None = None,
    ) -> None:
        self.session_id = session_id
        self.self_id = self_id
        self.ordering = ordering
        self._group = frozenset(members)
        self._deliver = deliver
        self._state = SessionState.CREATED
        self._issue_lock = threading.Lock()
        self._send_lock = threading.Lock()
        self._lock = threading.Lock()
        self._fifo_closed = False
        self._greeted: set[int] = set()
        self._finished: set[int] = set()
        self._everyone_ready = threading.Event()
        self._everyone_finished = threading.Event()
        self._protocol = ReliableFifoMulticast(
            round_id=session_id,
            self_id=self_id,
            members=members,
            channel=channel,
            deliver=self._on_fifo_deliver,
            policy=policy,
        )
        self._total_order = (
            TotalOrderMulticast(
                self_id=self_id, members=members, transmit=self._transmit, deliver=deliver
            )
            if ordering is Ordering.TOTAL
            else None
        )
        self._retransmitter = PeriodicTask(
            RETRANSMISSION_TICK, self._protocol.retransmit_due, f"retransmitter-{session_id}"
        )

    @property
    def size(self) -> int:
        return len(self._group)

    @property
    def state(self) -> SessionState:
        return self._state

    def handle(self, packet: Packet) -> None:
        self._protocol.handle(packet)

    def open(self, timeout: float) -> bool:
        with self._issue_lock, self._send_lock:
            if self._state is not SessionState.CREATED:
                raise RuntimeError(f"session {self.session_id} was already opened")
            self._retransmitter.start()
            self._protocol.multicast(Kind.HELLO)
        if not self._everyone_ready.wait(timeout):
            log.error("session %s: members did not join in time", self.session_id)
            return False
        with self._issue_lock:
            if self._state is SessionState.CREATED:
                self._state = SessionState.OPEN
        return True

    def multicast(self, payload: Any) -> None:
        with self._issue_lock:
            if self._state is not SessionState.OPEN:
                raise SessionClosed(f"session {self.session_id} is {self._state.name.lower()}")
            if self._total_order is not None:
                self._total_order.multicast(payload)
            else:
                self._transmit(payload)

    def close(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._issue_lock:
            if self._state in (SessionState.CLOSING, SessionState.CLOSED):
                raise RuntimeError(f"session {self.session_id} is already closing")
            self._state = SessionState.CLOSING
            if self._total_order is not None:
                self._total_order.announce_end()
        ordered = self._total_order is None or self._total_order.wait_until_everyone_ended(timeout)
        with self._send_lock:
            self._fifo_closed = True
            self._protocol.multicast(Kind.END)
        finished = self._everyone_finished.wait(_remaining(deadline))
        acknowledged = self._protocol.wait_until_acknowledged(_remaining(deadline))
        self._retransmitter.stop()
        self._state = SessionState.CLOSED
        clean = ordered and finished and acknowledged and not self._protocol.unreachable
        if not clean:
            log.error("session %s: did not close cleanly", self.session_id)
        return clean

    def stats(self, dropped_by_fault_injector: int = 0) -> TransportStats:
        counters = self._protocol.counters
        order = self._total_order.counters if self._total_order is not None else None
        return TransportStats(
            data_sent=counters.data_sent,
            retransmissions=counters.retransmissions,
            acks_sent=counters.acks_sent,
            duplicates_discarded=counters.duplicates_discarded,
            held_back=counters.held_back,
            dropped_by_fault_injector=dropped_by_fault_injector,
            unreachable_peers=tuple(sorted(self._protocol.unreachable)),
            order_messages_sent=order.messages_sent if order else 0,
            order_acks_sent=order.acks_sent if order else 0,
            order_queue_peak=order.queue_peak if order else 0,
        )

    def _transmit(self, payload: Any) -> None:
        with self._send_lock:
            if self._fifo_closed:
                log.debug("session %s: dropping message sent after END", self.session_id)
                return
            self._protocol.multicast(Kind.DATA, payload)

    def _on_fifo_deliver(self, packet: DataPacket) -> None:
        match packet.kind:
            case Kind.HELLO:
                with self._lock:
                    self._greeted.add(packet.sender)
                    if self._greeted >= self._group:
                        self._everyone_ready.set()
            case Kind.DATA if self._total_order is not None:
                self._total_order.receive(packet.sender, packet.payload)
            case Kind.DATA:
                self._deliver(packet.sender, packet.payload)
            case Kind.END:
                with self._lock:
                    self._finished.add(packet.sender)
                    if self._finished >= self._group:
                        self._everyone_finished.set()


def _remaining(deadline: float) -> float:
    return max(0.0, deadline - time.monotonic())
