"""引擎层单元测试：不经过 HTTP，直接核对状态机行为。"""

from __future__ import annotations

import pytest

from app.engine import InvalidPacketError, SwitchEngine, UnknownQueueError


def delivered_sequences(engine):
    return [(p.flow_id, p.seq, p.queue_id) for _, p in engine.egress]


def drain(engine, queue_id, rounds=20):
    for _ in range(rounds):
        outcome = engine.deliver(queue_id, count=100)
        if not outcome.delivered:
            return


def drain_all(engine, rounds=20):
    for _ in range(rounds):
        moved = False
        for q in range(engine.n_queues):
            moved = bool(engine.deliver(q, count=100).delivered) or moved
        if not moved:
            return


def test_head_of_line_blocks_only_same_flow():
    e = SwitchEngine(queues=2, max_pending=4)
    e.enqueue("a", 1, 0)
    e.enqueue("a", 2, 1)
    e.enqueue("b", 1, 1)
    out = e.deliver(1, count=10)
    assert [(p.flow_id, p.seq) for _, p in out.delivered] == [("b", 1)]
    assert e.stream_state("a")["delivered"] == []


def test_delivery_order_is_per_flow_even_with_interleaved_queues():
    e = SwitchEngine(queues=3, max_pending=8)
    layout = {1: 2, 2: 0, 3: 1, 4: 2, 5: 0}
    for seq, q in layout.items():
        e.enqueue("f", seq, q)
    drain_all(e)
    assert [seq for _, seq, _ in delivered_sequences(e)] == [1, 2, 3, 4, 5]


def test_capacity_boundary_exact():
    e = SwitchEngine(queues=2, max_pending=4)
    for seq in range(4):
        assert e.enqueue("f", seq, 0).accepted
    rejected = e.enqueue("f", 4, 0)
    assert not rejected.accepted and rejected.reason == "backlog_full"
    assert e.stream_state("f")["rejected"] == 1
    e.deliver(0, count=1)
    assert e.enqueue("f", 4, 0).accepted


def test_duplicate_and_stale():
    e = SwitchEngine(queues=2)
    assert e.enqueue("f", 1, 0).accepted
    assert e.enqueue("f", 1, 0).reason == "duplicate"
    e.deliver(0)
    assert e.enqueue("f", 1, 0).reason == "duplicate"
    assert e.enqueue("f", 0, 0).reason == "stale"
    assert e.stream_state("f")["delivered"] == [1]


def test_switch_only_affects_new_packets():
    e = SwitchEngine(queues=2)
    e.enqueue("f", 1)
    assert e.switch(1) == 0
    e.enqueue("f", 2)
    assert e.stream_state("f")["pending"] == [
        {"seq": 1, "queue_id": 0},
        {"seq": 2, "queue_id": 1},
    ]


def test_invalid_inputs():
    e = SwitchEngine(queues=1)
    with pytest.raises(UnknownQueueError):
        e.enqueue("f", 1, 1)
    with pytest.raises(InvalidPacketError):
        e.enqueue("f", -1, 0)
    with pytest.raises(InvalidPacketError):
        e.enqueue("bad flow", 1, 0)
    with pytest.raises(ValueError):
        e.deliver(0, count=0)
    with pytest.raises(ValueError):
        SwitchEngine(queues=0)
    with pytest.raises(ValueError):
        SwitchEngine(max_pending=0)


def test_empty_deliver_is_noop_and_idempotent():
    e = SwitchEngine(queues=2)
    out = e.deliver(0, count=5)
    assert out.delivered == [] and out.remaining == 0
    e.enqueue("f", 1, 0)
    assert e.deliver(0, count=10).delivered
    assert e.deliver(0, count=10).delivered == []


def test_delivery_within_one_call_respects_flow_order():
    # 一次推进里，前面的包交付后能立刻让同流后面的包在新队列可交付的情况下跟上
    e = SwitchEngine(queues=2)
    e.enqueue("f", 1, 0)
    e.enqueue("f", 2, 0)
    e.enqueue("f", 3, 0)
    out = e.deliver(0, count=10)
    assert [p.seq for _, p in out.delivered] == [1, 2, 3]


def test_interleaved_flows_keep_global_egress_monotonic_per_flow():
    e = SwitchEngine(queues=2)
    for i in range(1, 5):
        e.enqueue("x", i, i % 2)
        e.enqueue("y", i, (i + 1) % 2)
        e.enqueue("z", i, 0)
    drain_all(e)
    for flow in ("x", "y", "z"):
        seqs = [seq for f, seq, _ in delivered_sequences(e) if f == flow]
        assert seqs == sorted(seqs) == list(range(1, 5))
    # 上层接收的全局序号连续
    assert [o for o, _ in e.egress] == list(range(1, len(e.egress) + 1))


# --------------------------------------------------------------------------
# 接收队列推进逻辑：同队列轮转、拆调用等价、按新队头重判、O(1) 调度
# --------------------------------------------------------------------------


def _layout(engine, spec):
    """spec: {flow_id: [(seq, queue_id), ...]}"""
    for flow_id, packets in spec.items():
        for seq, queue_id in packets:
            outcome = engine.enqueue(flow_id, seq, queue_id)
            assert outcome.accepted, (flow_id, seq, outcome.reason)


def _egress(engine):
    return [(p.flow_id, p.seq, p.queue_id) for _, p in engine.egress]


def test_one_call_does_not_starve_other_flows_in_same_queue():
    e = SwitchEngine(queues=2, max_pending=16)
    _layout(e, {"busy": [(i, 0) for i in range(1, 9)], "other": [(1, 0)]})
    out = e.deliver(0, count=10)
    seqs = [(p.flow_id, p.seq) for _, p in out.delivered]
    # busy 持续可交付，但轮到它之前 other 先交付一包；busy 每轮最多一包
    assert seqs[:3] == [("busy", 1), ("other", 1), ("busy", 2)]
    # other 没有后续包，之后 busy 继续，每次一包，不会连交两包后重新排自己
    assert [s for f, s in seqs if f == "busy"] == list(range(1, 9))


def test_repeated_calls_rotate_fairly_and_preserve_position():
    e = SwitchEngine(queues=2, max_pending=16)
    _layout(e, {"a": [(1, 0), (2, 0), (3, 0)], "b": [(1, 0), (2, 0)]})
    order = []
    for _ in range(5):
        for _, packet in e.deliver(0, count=1).delivered:
            order.append((packet.flow_id, packet.seq))
    # a1, b1, a2, b2, a3：每次调用只交一包，跨调用继续轮转
    assert order == [("a", 1), ("b", 1), ("a", 2), ("b", 2), ("a", 3)]


def test_advance_split_into_calls_matches_single_call():
    # 积压相同、队列推进顺序相同、每次队列推进总量相同：一次调用与拆成几次调用，
    # 交付顺序完全一致（轮转位置跨调用保留）。
    spec = {
        "a": [(seq, seq % 2) for seq in range(1, 9)],
        "b": [(seq, (seq + 1) % 2) for seq in range(1, 9)],
        "c": [(seq, 0) for seq in range(1, 5)],
    }
    plan = [(1, 6), (0, 5), (1, 4), (0, 3)]  # (队列, 本轮推进量)

    single = SwitchEngine(queues=2, max_pending=16)
    _layout(single, spec)
    for queue_id, count in plan:
        single.deliver(queue_id, count=count)
    single_egress = _egress(single)

    for chunks in ([1, 1, 1], [2, 1], [3], [1, 2], [2, 3, 1]):
        split = SwitchEngine(queues=2, max_pending=16)
        _layout(split, spec)
        for queue_id, count in plan:
            remaining = count
            i = 0
            while remaining > 0:
                step = min(remaining, chunks[i % len(chunks)])
                split.deliver(queue_id, count=step)
                remaining -= step
                i += 1
        assert _egress(split) == single_egress, chunks


def test_same_total_count_split_differs_only_by_call_boundaries():
    # 同样的推进总量，一次调用与多次单包调用交付顺序一致
    def arrange():
        engine = SwitchEngine(queues=2, max_pending=16)
        _layout(
            engine,
            {
                "a": [(1, 0), (2, 0), (3, 1), (4, 1), (5, 0)],
                "b": [(1, 0), (2, 1), (3, 0)],
                "c": [(1, 1), (2, 1)],
            },
        )
        return engine

    one = arrange()
    one_out = []
    for q in (1, 0):  # 先推进队列 1，再推进队列 0，本队列内部轮转
        one_out.extend(one.deliver(q, count=4).delivered)
    one_out_egress = _egress(one)

    split = arrange()
    for q in (1, 0):
        for _ in range(4):
            split.deliver(q, count=1)
    assert _egress(split) == one_out_egress
    assert [(p.flow_id, p.seq) for _, p in one_out] == [
        (f, s) for f, s, _ in one_out_egress
    ]


def test_smaller_seq_arrival_retargets_ready_queue():
    e = SwitchEngine(queues=2, max_pending=4)
    e.enqueue("f", 2, 1)
    assert e.queue_view(1)["ready"] == 1

    # 收进比队头更小的包 1，队头变成队列 0 上的包 1
    assert e.enqueue("f", 1, 0).accepted
    assert e.queue_view(0)["ready"] == 1
    assert e.queue_view(1)["ready"] == 0
    assert e.deliver(1, count=10).delivered == []

    assert [p.seq for _, p in e.deliver(0, count=10).delivered] == [1]
    # 交付队头后马上按新队头（队列 1 上的包 2）重判
    assert e.queue_view(0)["ready"] == 0
    assert e.queue_view(1)["ready"] == 1
    assert [p.seq for _, p in e.deliver(1, count=10).delivered] == [2]


def test_delivering_head_reyhandles_both_queues():
    e = SwitchEngine(queues=2, max_pending=8)
    _layout(e, {"f": [(1, 0), (2, 1), (3, 0), (4, 1)]})
    assert e.queue_view(0)["ready"] == 1
    assert e.queue_view(1)["ready"] == 0

    assert [p.seq for _, p in e.deliver(0, count=10).delivered] == [1]
    assert e.queue_view(0)["ready"] == 0
    assert e.queue_view(1)["ready"] == 1

    assert [p.seq for _, p in e.deliver(1, count=10).delivered] == [2]
    assert e.queue_view(0)["ready"] == 1
    assert e.queue_view(1)["ready"] == 0


def test_remaining_includes_packets_blocked_behind_other_queue():
    e = SwitchEngine(queues=2, max_pending=8)
    _layout(e, {"f": [(1, 0), (2, 1), (3, 1)]})
    out = e.deliver(1, count=10)
    assert out.delivered == []
    assert out.remaining == 2  # 被队列 0 上的包 1 挡住，仍计入本队列
    e.deliver(0, count=10)
    out = e.deliver(1, count=10)
    assert [p.seq for _, p in out.delivered] == [2, 3]
    assert out.remaining == 0


def test_reset_restarts_rotation_from_scratch():
    spec = {
        "a": [(1, 0), (2, 0), (3, 0)],
        "b": [(1, 0), (2, 0)],
        "c": [(1, 0)],
    }
    fresh = SwitchEngine(queues=2, max_pending=8)
    _layout(fresh, spec)
    expected = [(p.flow_id, p.seq) for _, p in fresh.deliver(0, count=3).delivered]

    e = SwitchEngine(queues=2, max_pending=8)
    _layout(e, spec)
    e.deliver(0, count=2)  # 先推进一部分，轮转位置已经偏移
    e.reset()
    assert e.queue_pending_unlocked(0) == 0
    _layout(e, spec)
    actual = [(p.flow_id, p.seq) for _, p in e.deliver(0, count=3).delivered]
    assert actual == expected == [("a", 1), ("b", 1), ("c", 1)]


def test_scheduling_work_does_not_grow_with_idle_or_blocked_flows():
    class CountingEngine(SwitchEngine):
        def __init__(self, *args, **kwargs):
            self.sync_calls = 0
            super().__init__(*args, **kwargs)

        def _sync_ready(self, flow):
            self.sync_calls += 1
            super()._sync_ready(flow)

    e = CountingEngine(queues=3, max_pending=256)
    # 大量无积压流：只创建，不留下报文
    for i in range(2000):
        e._flow(f"idle-{i}")
    # 大量被其他队列挡住队头的流，全部堆在队列 1 上等候
    for i in range(2000):
        e.enqueue(f"blocked-{i}", 1, 0)
        e.enqueue(f"blocked-{i}", 2, 1)
    # 本队列持续可交付的流
    for seq in range(1, 201):
        e.enqueue("busy", seq, 2)

    e.sync_calls = 0
    out = e.deliver(2, count=100)
    assert len(out.delivered) == 100
    # 每交付一个包最多两次挂队操作（出队 + 按新队头重挂），与 4000 条无关流无关
    assert e.sync_calls <= 2 * 100
    assert e.queue_pending_unlocked(1) == 2000
    assert e.queue_pending_unlocked(2) == 100

