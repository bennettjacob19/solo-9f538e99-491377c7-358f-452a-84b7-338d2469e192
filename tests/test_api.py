from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import create_app


@pytest.fixture()
def client():
    app = create_app(queues=2, max_pending=4)
    with TestClient(app) as c:
        yield c


def send(client, flow_id, seq, queue_id):
    return client.post(
        "/packets", json={"flow_id": flow_id, "seq": seq, "queue_id": queue_id}
    )


def advance(client, queue_id, count=1):
    return client.post(f"/queues/{queue_id}/advance", json={"count": count})


def egress(client, flow_id=None):
    params = {"flow_id": flow_id} if flow_id else {}
    return [
        (r["flow_id"], r["seq"], r["queue_id"])
        for r in client.get("/egress", params=params).json()["egress"]
    ]


def test_docs_available(client):
    assert client.get("/docs").status_code == 200
    assert client.get("/openapi.json").status_code == 200
    assert client.get("/", follow_redirects=False).status_code in (307, 308)


def test_switch_keeps_old_packets_in_old_queue(client):
    assert send(client, "f1", 1, 0).status_code == 201
    assert client.post("/switch", json={"queue_id": 1}).json()["previous_queue"] == 0
    assert send(client, "f1", 2, 1).status_code == 201

    # 新队列上的后续包不能越过原队列上的前序包
    out = advance(client, 1).json()
    assert out["delivered"] == []
    assert out["remaining"] == 1

    # 推进原队列，包 1 交付
    out = advance(client, 0).json()
    assert [(d["seq"], d["queue_id"]) for d in out["delivered"]] == [(1, 0)]

    # 前序交付后，新队列才能把后续包交给上层
    out = advance(client, 1).json()
    assert [(d["seq"], d["queue_id"]) for d in out["delivered"]] == [(2, 1)]
    assert egress(client, "f1") == [("f1", 1, 0), ("f1", 2, 1)]


def test_per_flow_in_order_delivery(client):
    # 人为把流内包号打乱到两条队列
    send(client, "f1", 1, 1)
    send(client, "f1", 2, 0)
    send(client, "f1", 3, 1)
    send(client, "f1", 4, 0)

    advance(client, 0, count=10)
    assert egress(client, "f1") == []  # 队头 1 在队列 1

    advance(client, 1, count=10)
    assert egress(client, "f1") == [("f1", 1, 1)]

    # 队列 0 此时可以交付 2，然后队列 1 交付 3，依次交错
    advance(client, 0, count=10)
    assert egress(client, "f1") == [("f1", 1, 1), ("f1", 2, 0)]
    advance(client, 1, count=10)
    advance(client, 0, count=10)
    assert [e[1] for e in egress(client, "f1")] == [1, 2, 3, 4]


def test_other_flows_are_not_blocked(client):
    send(client, "slow", 1, 0)
    send(client, "slow", 2, 1)
    send(client, "fast", 1, 1)
    send(client, "fast", 2, 1)

    out = advance(client, 1, count=10).json()
    assert [(d["flow_id"], d["seq"]) for d in out["delivered"]] == [
        ("fast", 1),
        ("fast", 2),
    ]
    assert egress(client, "slow") == []
    assert egress(client, "fast") == [("fast", 1, 1), ("fast", 2, 1)]


def test_backlog_limit_is_per_flow(client):
    for seq in range(1, 5):
        assert send(client, "f1", seq, 0).status_code == 201
    resp = send(client, "f1", 5, 0)
    assert resp.status_code == 503
    body = resp.json()
    assert body["accepted"] is False and body["rejected"] is True
    assert body["reason"] == "backlog_full"

    # 其他流不受影响
    assert send(client, "f2", 1, 0).status_code == 201

    # 腾出位置后重试同一个包
    advance(client, 0).json()
    assert send(client, "f1", 5, 0).status_code == 201


def test_repeated_switching_with_backlog_on_both_sides(client):
    # 来回切换，让积压同时分散在两条队列上
    assert send(client, "f1", 1, 0).status_code == 201
    assert client.post("/switch", json={"queue_id": 1}).json()["active_queue"] == 1
    assert send(client, "f1", 2, 1).status_code == 201
    assert client.post("/switch", json={"queue_id": 0}).json()["active_queue"] == 0
    assert send(client, "f1", 3, 0).status_code == 201
    assert client.post("/switch", json={"queue_id": 1}).json()["active_queue"] == 1
    assert send(client, "f1", 4, 1).status_code == 201

    state = client.get("/state").json()
    assert state["queues"][0]["pending"] == 2
    assert state["queues"][1]["pending"] == 2
    assert state["queues"][1]["ready"] == 0

    # 先推进新队列：包 2 被前序包 1 挡住，什么也交不出去
    assert advance(client, 1, count=10).json()["delivered"] == []

    # 推进原队列：只交付包 1，包 3 仍被包 2 挡住
    assert [d["seq"] for d in advance(client, 0, count=10).json()["delivered"]] == [1]
    # 新队列接上包 2，随后包 4 又被包 3 挡住
    assert [d["seq"] for d in advance(client, 1, count=10).json()["delivered"]] == [2]
    assert [d["seq"] for d in advance(client, 0, count=10).json()["delivered"]] == [3]
    assert [d["seq"] for d in advance(client, 1, count=10).json()["delivered"]] == [4]

    assert [s for _, s, _ in egress(client, "f1")] == [1, 2, 3, 4]


def test_count_bounds_advance(client):
    for seq in range(1, 4):
        send(client, "f1", seq, 0)
    out = advance(client, 0, count=2).json()
    assert len(out["delivered"]) == 2
    assert out["remaining"] == 1
    out = advance(client, 0).json()
    assert len(out["delivered"]) == 1
    assert out["remaining"] == 0


def test_duplicate_and_stale_rejected(client):
    assert send(client, "f1", 1, 0).status_code == 201
    assert send(client, "f1", 1, 0).status_code == 409
    advance(client, 0)
    assert send(client, "f1", 1, 0).status_code == 409
    assert send(client, "f1", 0, 0).status_code == 409


def test_unknown_queue_404(client):
    assert send(client, "f1", 1, 5).status_code == 404
    assert advance(client, 5).status_code == 404
    assert client.post("/switch", json={"queue_id": 9}).status_code == 404


def test_state_and_queue_views(client):
    send(client, "f1", 1, 0)
    send(client, "f1", 2, 1)
    state = client.get("/state").json()
    assert state["config"]["active_queue"] == 0
    assert state["config"]["max_pending_per_flow"] == 4
    q0 = state["queues"][0]
    q1 = state["queues"][1]
    assert q0["pending"] == 1 and q0["ready"] == 1
    assert q1["pending"] == 1 and q1["ready"] == 0
    assert q1["blocked_by_other_queue"] == 1
    assert state["streams"][0]["head"] == {"seq": 1, "queue_id": 0}


def test_reset(client):
    send(client, "f1", 1, 0)
    advance(client, 0)
    client.post("/reset")
    assert client.get("/egress").json()["egress"] == []
    assert client.get("/state").json()["streams"] == []


def test_advance_rotates_between_flows_in_same_queue(client):
    # 一条流持续可交付时，同一队列里其他流在它下一轮之前也各交付一包
    for seq in range(1, 5):
        send(client, "busy", seq, 0)
    send(client, "other", 1, 0)

    first = advance(client, 0, count=1).json()["delivered"]
    assert [(d["flow_id"], d["seq"]) for d in first] == [("busy", 1)]

    out = advance(client, 0, count=1).json()["delivered"]
    assert [(d["flow_id"], d["seq"]) for d in out] == [("other", 1)]

    out = advance(client, 0, count=1).json()["delivered"]
    assert [(d["flow_id"], d["seq"]) for d in out] == [("busy", 2)]


def test_smaller_seq_arrival_moves_readiness_to_its_queue(client):
    send(client, "f1", 2, 1)
    assert client.get("/queues").json()["queues"][1]["ready"] == 1

    # 收进比当前队头更小的包 1：队头变成队列 0 上的包 1
    send(client, "f1", 1, 0)
    queues = client.get("/queues").json()["queues"]
    assert queues[0]["ready"] == 1
    assert queues[1]["ready"] == 0

    assert advance(client, 1, count=10).json()["delivered"] == []
    assert [d["seq"] for d in advance(client, 0, count=10).json()["delivered"]] == [1]
    # 交付队头后立即按新队头重判：队列 1 又能交付包 2
    assert [d["seq"] for d in advance(client, 1, count=10).json()["delivered"]] == [2]


def test_remaining_counts_blocked_packets(client):
    send(client, "f1", 1, 0)
    send(client, "f1", 2, 1)
    send(client, "f1", 3, 1)
    out = advance(client, 1, count=10).json()
    assert out["delivered"] == []
    assert out["remaining"] == 2


def test_same_backlog_split_advance_is_deterministic(client):
    layout = [
        ("a", 1, 0),
        ("b", 1, 0),
        ("a", 2, 0),
        ("b", 2, 0),
        ("a", 3, 0),
        ("c", 1, 0),
    ]
    fresh = create_app(queues=2, max_pending=8)
    with TestClient(fresh) as c2:
        for flow_id, seq, queue_id in layout:
            send(c2, flow_id, seq, queue_id)
        one_call = advance(c2, 0, count=5).json()["delivered"]

    with TestClient(create_app(queues=2, max_pending=8)) as c3:
        for flow_id, seq, queue_id in layout:
            send(c3, flow_id, seq, queue_id)
        split = []
        for _ in range(5):
            split.extend(advance(c3, 0, count=1).json()["delivered"])

    assert [(d["flow_id"], d["seq"]) for d in split] == [
        (d["flow_id"], d["seq"]) for d in one_call
    ]
