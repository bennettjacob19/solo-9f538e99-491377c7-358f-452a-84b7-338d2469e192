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
