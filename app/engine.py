"""接收队列切换演练的核心状态机（与 Web 框架无关，方便单独测试）。

模型要点
--------
* 一个网卡有多条接收队列（默认 2 条）。报文被哪条队列「收下」在接收时就定下来，
  之后永远由这条队列向上一层交付。
* 每条流按包号顺序交付。包号最小的未交付报文是队头；队头还在等前序包时，这条流
  后面的报文（即使已在别的队列里）都要等候，其他流不受影响。
* 每条队列内部按流轮转（round-robin）：一条流每轮最多交付一个包，然后就排到队尾。
  只要一条流持续可交付，轮到它之前，其他可交付的流各最多交付一个包，不会被它饿死。
  轮转位置跨 ``advance`` 调用保留，因此同样的积压、同样多的推进，拆成几次调用与
  一次调用得到相同的交付顺序。
* 一条流当前只有「队头所在的队列」能交付它。收进比队头包号更小的报文会让队头变化，
  交付队头后队头也会后移；两种情况都立即按新队头把这条流挂到对应队列的轮转队列里，
  并从原队列的轮转队列中摘下。
* 一条流最多积压 ``max_pending`` 个「已接收但未交付」的报文；满了只拒收这一流的
  新报文，别的流照常收包。推进队列腾出位置后可以重试。
* 切换接收队列只影响之后新到的报文；已经收下的报文仍留在原队列，由原队列交付。
* 收包和推进只碰「当前队列的轮转队列」和增量计数器，调度开销不随无积压流、
  被其他队列阻塞的流的数量增长。
"""

from __future__ import annotations

import bisect
from collections import OrderedDict
from dataclasses import dataclass, field
from threading import RLock
from typing import Dict, List, Optional, Tuple

DEFAULT_QUEUES = 2
DEFAULT_MAX_PENDING = 4


class UnknownQueueError(ValueError):
    """队列编号越界。"""


class UnknownFlowError(KeyError):
    """流不存在。"""


class InvalidPacketError(ValueError):
    """报文参数不合法。"""


def _clean_flow_id(flow_id: str) -> str:
    if not isinstance(flow_id, str) or not flow_id.strip():
        raise InvalidPacketError("flow_id 不能为空")
    if any(ch.isspace() for ch in flow_id):
        raise InvalidPacketError("flow_id 不能包含空白字符")
    if len(flow_id) > 64:
        raise InvalidPacketError("flow_id 最长 64 个字符")
    return flow_id


@dataclass
class Packet:
    flow_id: str
    seq: int
    queue_id: int


@dataclass
class Flow:
    flow_id: str
    pending: List[Packet] = field(default_factory=list)
    delivered: List[int] = field(default_factory=list)
    delivered_set: set = field(default_factory=set)
    rejected: int = 0
    duplicate_attempts: int = 0
    stale_attempts: int = 0
    # 这条流当前挂在哪个队列的轮转队列里；None 表示没有积压、未挂队。
    ready_queue: Optional[int] = field(default=None, repr=False, compare=False)


@dataclass
class EnqueueOutcome:
    accepted: bool
    reason: str
    detail: str
    flow: "Flow"
    packet: Packet
    capacity: int


@dataclass
class DeliveryOutcome:
    queue_id: int
    delivered: List[Tuple[int, Packet]]  # (全局交付序号, 报文)
    remaining: int


class SwitchEngine:
    def __init__(
        self,
        queues: int = DEFAULT_QUEUES,
        max_pending: int = DEFAULT_MAX_PENDING,
    ) -> None:
        if queues < 1:
            raise ValueError("queues 至少为 1")
        if max_pending < 1:
            raise ValueError("max_pending 至少为 1")
        self.n_queues = queues
        self.max_pending = max_pending
        self._lock = RLock()
        self._reset_unlocked()

    # ------------------------------------------------------------------ 内部
    def _reset_unlocked(self) -> None:
        self.flows: Dict[str, Flow] = {}
        self.active_queue: int = 0
        self.egress: List[Tuple[int, Packet]] = []
        # 每条队列一个轮转队列：队头就在本队列、随时可交付的流，按轮转顺序排列。
        self._ready: List["OrderedDict[str, None]"] = [
            OrderedDict() for _ in range(self.n_queues)
        ]
        # 每条队列里已接收未交付的报文数（含被其他队列挡住的），O(1) 维护。
        self._queue_pending: List[int] = [0] * self.n_queues

    def _check_queue(self, queue_id: int) -> int:
        if isinstance(queue_id, bool) or not isinstance(queue_id, int):
            raise UnknownQueueError(f"队列编号必须是整数：{queue_id!r}")
        if not 0 <= queue_id < self.n_queues:
            raise UnknownQueueError(
                f"队列 {queue_id} 不存在，有效范围 0..{self.n_queues - 1}"
            )
        return queue_id

    def _flow(self, flow_id: str) -> Flow:
        flow_id = _clean_flow_id(flow_id)
        flow = self.flows.get(flow_id)
        if flow is None:
            flow = Flow(flow_id=flow_id)
            self.flows[flow_id] = flow
        return flow

    @staticmethod
    def _head(flow: Flow) -> Optional[Packet]:
        if not flow.pending:
            return None
        return flow.pending[0]  # pending 始终按包号升序

    def _sync_ready(self, flow: Flow) -> None:
        """按当前队头把流挂到正确的轮转队列（或摘下）。

        只做 O(1) 的字典操作，不扫描其他流：流过期/无积压时摘下，
        队头换队列时摘旧挂新，仍然可交付时保持原轮转位置。
        交付队头后调用会把它重新排到队尾，形成轮转。
        """
        head = self._head(flow)
        target = head.queue_id if head is not None else None
        current = flow.ready_queue
        if current == target:
            return
        if current is not None:
            self._ready[current].pop(flow.flow_id, None)
        if target is not None:
            self._ready[target][flow.flow_id] = None
        flow.ready_queue = target

    # ------------------------------------------------------------------ 操作
    def reset(self) -> None:
        with self._lock:
            self._reset_unlocked()

    def ensure_flow(self, flow_id: str) -> Flow:
        with self._lock:
            return self._flow(flow_id)

    def switch(self, queue_id: int) -> int:
        """把新报文的默认接收队列换成 ``queue_id``，返回切换前的队列。

        已经在各队列里的报文不受影响，仍由收下它们的队列交付；可以反复切换。
        """
        with self._lock:
            previous = self.active_queue
            self.active_queue = self._check_queue(queue_id)
            return previous

    def enqueue(
        self, flow_id: str, seq: int, queue_id: Optional[int] = None
    ) -> EnqueueOutcome:
        """让 ``queue_id`` 收下这条新报文；不指定就交给当前接收队列。"""
        with self._lock:
            flow = self._flow(flow_id)
            if isinstance(seq, bool) or not isinstance(seq, int):
                raise InvalidPacketError("seq 必须是整数")
            if seq < 0:
                raise InvalidPacketError("seq 不能为负数")
            if queue_id is None:
                queue_id = self.active_queue
            self._check_queue(queue_id)

            if any(p.seq == seq for p in flow.pending) or seq in flow.delivered_set:
                flow.duplicate_attempts += 1
                return EnqueueOutcome(
                    accepted=False,
                    reason="duplicate",
                    detail=f"流 {flow.flow_id} 的包 {seq} 已经收到过，按重复包拒收",
                    flow=flow,
                    packet=Packet(flow.flow_id, seq, queue_id),
                    capacity=self.max_pending,
                )

            if flow.delivered and seq < flow.delivered[-1]:
                flow.stale_attempts += 1
                return EnqueueOutcome(
                    accepted=False,
                    reason="stale",
                    detail=(
                        f"流 {flow.flow_id} 已经交付到包 {flow.delivered[-1]}，"
                        f"包 {seq} 来晚了，无法再按顺序交付"
                    ),
                    flow=flow,
                    packet=Packet(flow.flow_id, seq, queue_id),
                    capacity=self.max_pending,
                )

            if len(flow.pending) >= self.max_pending:
                flow.rejected += 1
                return EnqueueOutcome(
                    accepted=False,
                    reason="backlog_full",
                    detail=(
                        f"流 {flow.flow_id} 已积压 {len(flow.pending)} 个包"
                        f"（上限 {self.max_pending}），只拒收这一流的新包；"
                        "推进队列腾出位置后可重试"
                    ),
                    flow=flow,
                    packet=Packet(flow.flow_id, seq, queue_id),
                    capacity=self.max_pending,
                )

            packet = Packet(flow.flow_id, seq, queue_id)
            # 保持 pending 按包号升序，队头恒为 pending[0]（积压上限很小）。
            insert_at = bisect.bisect_left([p.seq for p in flow.pending], seq)
            flow.pending.insert(insert_at, packet)
            self._queue_pending[queue_id] += 1
            # 新包可能成为更小的队头：按当前队头重新判断这条流由哪条队列交付。
            self._sync_ready(flow)
            return EnqueueOutcome(
                accepted=True,
                reason="accepted",
                detail=f"包 {seq} 由队列 {queue_id} 收下，等待按流内包号顺序交付",
                flow=flow,
                packet=packet,
                capacity=self.max_pending,
            )

    def deliver(self, queue_id: int, count: int = 1) -> DeliveryOutcome:
        """单独推进一条队列，最多向上一层交付 ``count`` 个报文。

        本队列的轮转队列里每次取一条流交付它的队头，然后排到队尾：一条流每轮
        最多交付一个包，持续可交付的流也不会饿死其他流。队头卡在别的队列的流
        不在本队列的轮转队列里，完全不参与本轮扫描。
        """
        with self._lock:
            self._check_queue(queue_id)
            if isinstance(count, bool) or not isinstance(count, int) or count < 1:
                raise ValueError("count 必须是大于等于 1 的整数")

            delivered: List[Tuple[int, Packet]] = []
            ready = self._ready[queue_id]
            while len(delivered) < count and ready:
                flow_id, _ = ready.popitem(last=False)
                flow = self.flows[flow_id]
                # 出队后到交付完成前，这条流不挂在任何轮转队列里。
                flow.ready_queue = None
                head = self._head(flow)
                if head is None:
                    continue
                if head.queue_id != queue_id:
                    # 防御：理论上轮转队列里的流队头一定在本队列。
                    self._sync_ready(flow)
                    continue

                flow.pending.pop(0)
                flow.delivered.append(head.seq)
                flow.delivered_set.add(head.seq)
                self._queue_pending[queue_id] -= 1
                order = len(self.egress) + 1
                self.egress.append((order, head))
                delivered.append((order, head))
                # 按交付后的队头重新挂队：仍在本次队列就排到队尾继续轮转，
                # 队头换到别的队列就挂到那条队列的队尾。
                self._sync_ready(flow)

            return DeliveryOutcome(
                queue_id=queue_id,
                delivered=delivered,
                remaining=self.queue_pending_unlocked(queue_id),
            )

    # ------------------------------------------------------------- 只读查询
    def queue_pending_unlocked(self, queue_id: int) -> int:
        """本队列未交付的报文数，包含被其他队列挡住队头的报文。"""
        with self._lock:
            return self._queue_pending[queue_id]

    def _queue_blocked_count(self, queue_id: int) -> int:
        """本队列里因前序包在别的队列而暂时无法交付的报文数。"""
        with self._lock:
            blocked = 0
            for flow in self.flows.values():
                waiting_on_other_queue = False
                for packet in flow.pending:  # 已按包号升序
                    if waiting_on_other_queue:
                        if packet.queue_id == queue_id:
                            blocked += 1
                    elif packet.queue_id != queue_id:
                        waiting_on_other_queue = True
            return blocked

    def queue_view(self, queue_id: int) -> dict:
        with self._lock:
            self._check_queue(queue_id)
            pending = self._queue_pending[queue_id]
            blocked = self._queue_blocked_count(queue_id)
            flow_counts: Dict[str, int] = {}
            packets = []
            for flow in self.flows.values():
                head = self._head(flow)
                for packet in flow.pending:  # 已按包号升序
                    if packet.queue_id == queue_id:
                        flow_counts[flow.flow_id] = (
                            flow_counts.get(flow.flow_id, 0) + 1
                        )
                        packets.append(
                            {
                                "flow_id": packet.flow_id,
                                "seq": packet.seq,
                                "is_flow_head": packet is head,
                            }
                        )
            return {
                "id": queue_id,
                "pending": pending,
                "ready": pending - blocked,
                "blocked_by_other_queue": blocked,
                "receiving": queue_id == self.active_queue,
                "flows": flow_counts,
                "packets": packets,
            }

    def stream_view(self, flow: Flow) -> dict:
        with self._lock:
            pending = flow.pending  # 始终保持包号升序
            head = pending[0] if pending else None
            return {
                "flow_id": flow.flow_id,
                "pending": [
                    {"seq": p.seq, "queue_id": p.queue_id} for p in pending
                ],
                "head": (
                    {"seq": head.seq, "queue_id": head.queue_id}
                    if head is not None
                    else None
                ),
                "pending_count": len(pending),
                "capacity": self.max_pending,
                "waiting_behind_head": max(0, len(pending) - 1),
                "delivered": list(flow.delivered),
                "next_expected": (
                    flow.delivered[-1] + 1 if flow.delivered else None
                ),
                "rejected": flow.rejected,
                "duplicate_attempts": flow.duplicate_attempts,
                "stale_attempts": flow.stale_attempts,
            }

    def egress_view(self, flow_id: Optional[str] = None) -> List[dict]:
        with self._lock:
            if flow_id is not None:
                _clean_flow_id(flow_id)
            return [
                {
                    "order": order,
                    "flow_id": packet.flow_id,
                    "seq": packet.seq,
                    "queue_id": packet.queue_id,
                }
                for order, packet in self.egress
                if flow_id is None or packet.flow_id == flow_id
            ]

    def state(self) -> dict:
        with self._lock:
            return {
                "config": {
                    "queues": self.n_queues,
                    "active_queue": self.active_queue,
                    "max_pending_per_flow": self.max_pending,
                },
                "streams": [self.stream_view(f) for f in self.flows.values()],
                "queues": [self.queue_view(q) for q in range(self.n_queues)],
                "egress": self.egress_view(),
            }

    def stream_state(self, flow_id: str) -> dict:
        with self._lock:
            flow = self.flows.get(flow_id)
            if flow is None:
                raise UnknownFlowError(flow_id)
            return self.stream_view(flow)
