"""Pydantic 模型：给 /docs 的请求/响应提供清晰结构、示例和校验。"""

from __future__ import annotations

from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

RejectReason = Literal["duplicate", "stale", "backlog_full"]


class PacketView(BaseModel):
    seq: int = Field(..., ge=0)
    queue_id: int = Field(..., ge=0, description="收下这个报文的队列")


class StreamSnapshot(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "flow_id": "f1",
                "pending": [
                    {"seq": 2, "queue_id": 0},
                    {"seq": 3, "queue_id": 1},
                ],
                "head": {"seq": 2, "queue_id": 0},
                "pending_count": 2,
                "capacity": 4,
                "waiting_behind_head": 1,
                "delivered": [0, 1],
                "next_expected": 2,
                "rejected": 0,
                "duplicate_attempts": 0,
                "stale_attempts": 0,
            }
        }
    )

    flow_id: str
    pending: List[PacketView] = Field(..., description="已接收但未交付的报文，按包号排序")
    head: Optional[PacketView] = Field(None, description="队头；它所在的队列才能推进这条流")
    pending_count: int = Field(..., ge=0)
    capacity: int = Field(..., ge=1, description="每条流允许的最大积压数")
    waiting_behind_head: int = Field(..., ge=0, description="队头之后的包，正等候交付")
    delivered: List[int] = Field(..., description="已按顺序交付给上层的包号")
    next_expected: Optional[int] = None
    rejected: int = Field(..., ge=0, description="因积压满额被拒收的包数")
    duplicate_attempts: int = Field(..., ge=0)
    stale_attempts: int = Field(..., ge=0)


class QueueSnapshot(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "id": 1,
                "pending": 2,
                "ready": 1,
                "blocked_by_other_queue": 1,
                "receiving": False,
                "flows": {"f1": 2},
                "packets": [
                    {"flow_id": "f1", "seq": 3, "is_flow_head": False}
                ],
            }
        }
    )

    id: int
    pending: int = Field(..., ge=0, description="队列里未交付的报文数")
    ready: int = Field(..., ge=0, description="此刻推进本队列可交付的报文数")
    blocked_by_other_queue: int = Field(
        ..., ge=0, description="因同流前序包还在别的队列而等候的报文数"
    )
    receiving: bool = Field(..., description="新报文是否默认进这条队列")
    flows: Dict[str, int] = Field(..., description="按流统计的积压数")
    packets: List[dict] = Field(..., description="队列内报文，按流内包号排序")


class EgressRecord(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {"order": 3, "flow_id": "f1", "seq": 2, "queue_id": 0}
        }
    )

    order: int = Field(..., ge=1, description="上层收到的全局顺序号")
    flow_id: str
    seq: int
    queue_id: int


class EnqueueRequest(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"example": {"flow_id": "f1", "seq": 3, "queue_id": 1}}
    )

    flow_id: str = Field(..., min_length=1, max_length=64, pattern=r"^\S+$")
    seq: int = Field(..., ge=0, description="包号；同一流内按它判断先后")
    queue_id: Optional[int] = Field(
        None,
        ge=0,
        description=(
            "由哪条接收队列收下；不传就用当前接收队列。"
            "收下之后这个包永远由该队列交付"
        ),
    )


class EnqueueResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "accepted": True,
                "flow_id": "f1",
                "seq": 3,
                "queue_id": 1,
                "reason": "accepted",
                "detail": "包 3 由队列 1 收下，等待按流内包号顺序交付",
                "pending": 2,
                "capacity": 4,
                "rejected": False,
            }
        }
    )

    accepted: bool
    flow_id: str
    seq: int
    queue_id: int
    reason: Literal["accepted", "duplicate", "stale", "backlog_full"]
    detail: str
    pending: int = Field(..., ge=0, description="该流当前的积压数")
    capacity: int = Field(..., ge=1)
    rejected: bool = Field(
        ..., description="是否因积压满额被拒；腾出位置后可重试同一个包"
    )


class DeliverRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={"example": {"count": 4}})

    count: int = Field(
        1,
        ge=1,
        le=1000,
        description="本次最多交付几个可交付报文；没有可交付的就提前结束",
    )


class DeliveredPacket(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={"example": {"flow_id": "f1", "seq": 2, "queue_id": 0}}
    )

    flow_id: str
    seq: int
    queue_id: int


class DeliverResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "queue_id": 0,
                "delivered": [{"flow_id": "f1", "seq": 2, "queue_id": 0}],
                "remaining": 0,
            }
        }
    )

    queue_id: int
    delivered: List[DeliveredPacket] = Field(
        ..., description="本次交付给上层的报文，按交付先后排列"
    )
    remaining: int = Field(..., ge=0, description="推进后该队列剩余的报文数")


class SwitchRequest(BaseModel):
    model_config = ConfigDict(json_schema_extra={"example": {"queue_id": 1}})

    queue_id: int = Field(..., ge=0, description="新的接收队列；报文须已在某条队列里")


class SwitchResponse(BaseModel):
    previous_queue: int
    active_queue: int
    detail: str
    queues: List[QueueSnapshot] = Field(
        ..., description="切换后各队列的积压情况；老报文仍留在原队列"
    )


class ResetResponse(BaseModel):
    detail: str = "已清空所有队列、流和上层交付记录"


class StateResponse(BaseModel):
    config: dict = Field(..., description="队列数、当前接收队列和每条流的容量")
    streams: List[StreamSnapshot]
    queues: List[QueueSnapshot]
    egress: List[EgressRecord] = Field(..., description="上层已收到的全部报文，按交付顺序")
