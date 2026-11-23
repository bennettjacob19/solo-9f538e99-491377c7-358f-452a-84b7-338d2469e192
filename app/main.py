"""FastAPI 入口：用 /docs 演练「切换接收队列时的乱序与按流保序」。

快速上手
--------
1. ``POST /packets`` 送一个包，指定 ``flow_id``、``seq`` 和收下它的 ``queue_id``。
2. ``POST /queues/{queue_id}/advance`` 单独推进某条队列，看上层这次收到哪些包。
3. ``POST /switch`` 换新报文的默认接收队列，再送包；老包仍由原队列交付。
"""

from __future__ import annotations

import os
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse

from .engine import (
    DEFAULT_MAX_PENDING,
    DEFAULT_QUEUES,
    InvalidPacketError,
    SwitchEngine,
    UnknownFlowError,
    UnknownQueueError,
)
from .models import (
    DeliverRequest,
    DeliverResponse,
    DeliveredPacket,
    EnqueueRequest,
    EnqueueResponse,
    ResetResponse,
    SwitchRequest,
    SwitchResponse,
    StateResponse,
)

DESCRIPTION = """
模拟网卡换接收队列时的乱序，并验证**每条流按收包顺序交付**。

* 每个报文在接收时绑定到当时收下它的队列，之后永远由这条队列交付；
  换队列只影响之后新到的报文。
* 上层按流内包号顺序收包：队头包没处理完时，同一条流的后续包等候，
  其他流照常交付。
* 每条流最多积压 `MAX_PENDING_PER_FLOW`（默认 4）个已接收但未交付的包，
  满了只拒收这一流的新包；推进队列腾出位置后可以重试。
* 任意队列可以单独推进，接收队列可以反复切换。
"""


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError as exc:  # pragma: no cover - 启动期配置错误
        raise RuntimeError(f"{name} 必须是整数，收到 {raw!r}") from exc
    if value < 1:
        raise RuntimeError(f"{name} 必须大于等于 1，收到 {value}")
    return value


def create_app(
    queues: Optional[int] = None, max_pending: Optional[int] = None
) -> FastAPI:
    n_queues = queues if queues is not None else _int_env("QUEUES", DEFAULT_QUEUES)
    capacity = (
        max_pending
        if max_pending is not None
        else _int_env("MAX_PENDING_PER_FLOW", DEFAULT_MAX_PENDING)
    )
    engine = SwitchEngine(queues=n_queues, max_pending=capacity)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield

    app = FastAPI(
        title="网卡接收队列切换演练",
        description=DESCRIPTION,
        version="1.0.0",
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    # ------------------------------------------------------------- 异常映射
    @app.exception_handler(UnknownQueueError)
    async def _unknown_queue(request: Request, exc: UnknownQueueError):
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(UnknownFlowError)
    async def _unknown_flow(request: Request, exc: UnknownFlowError):
        return JSONResponse(
            status_code=404, content={"detail": f"流 {exc.args[0]} 不存在"}
        )

    @app.exception_handler(InvalidPacketError)
    async def _invalid_packet(request: Request, exc: InvalidPacketError):
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    # ------------------------------------------------------------------ 页面
    @app.get("/", include_in_schema=False)
    def root() -> RedirectResponse:
        return RedirectResponse(url="/docs")

    @app.get("/healthz", tags=["运维"], summary="健康检查")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.get(
        "/state",
        response_model=StateResponse,
        tags=["查询"],
        summary="查看全部队列、流积压与上层已收报文",
    )
    def state() -> dict:
        return engine.state()

    @app.get("/queues", tags=["查询"], summary="查看每条队列的积压与可交付情况")
    def queues() -> dict:
        return {"queues": [engine.queue_view(q) for q in range(engine.n_queues)]}

    @app.get("/flows/{flow_id}", tags=["查询"], summary="查看单条流的状态")
    def flow_state(flow_id: str) -> dict:
        return engine.stream_state(flow_id)

    @app.get(
        "/egress",
        tags=["查询"],
        summary="查看上层收到的报文顺序（可按流过滤）",
    )
    def egress(
        flow_id: Optional[str] = Query(None, description="只看这条流的交付顺序")
    ) -> dict:
        return {"egress": engine.egress_view(flow_id)}

    # ------------------------------------------------------------------ 操作
    @app.post(
        "/packets",
        response_model=EnqueueResponse,
        status_code=201,
        tags=["操作"],
        summary="送入一个报文，指定由哪条队列收下",
        responses={
            409: {"description": "重复包或过期包，拒收"},
            503: {"description": "该流积压已达上限，只拒收这一流"},
        },
    )
    def receive_packet(body: EnqueueRequest):
        try:
            outcome = engine.enqueue(body.flow_id, body.seq, body.queue_id)
        except InvalidPacketError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except UnknownQueueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc

        response = EnqueueResponse(
            accepted=outcome.accepted,
            flow_id=outcome.flow.flow_id,
            seq=body.seq,
            queue_id=outcome.packet.queue_id,
            reason=outcome.reason,
            detail=outcome.detail,
            pending=len(outcome.flow.pending),
            capacity=outcome.capacity,
            rejected=outcome.reason == "backlog_full",
        )
        if outcome.accepted:
            return response
        status = 503 if outcome.reason == "backlog_full" else 409
        return JSONResponse(status_code=status, content=response.model_dump())

    @app.post(
        "/queues/{queue_id}/advance",
        response_model=DeliverResponse,
        tags=["操作"],
        summary="单独推进一条队列，交付当前可交付的报文",
    )
    def advance_queue(queue_id: int, body: Optional[DeliverRequest] = None) -> dict:
        count = body.count if body is not None else 1
        try:
            outcome = engine.deliver(queue_id, count=count)
        except UnknownQueueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "queue_id": outcome.queue_id,
            "delivered": [
                DeliveredPacket(
                    flow_id=packet.flow_id,
                    seq=packet.seq,
                    queue_id=packet.queue_id,
                )
                for _, packet in outcome.delivered
            ],
            "remaining": outcome.remaining,
        }

    @app.post(
        "/switch",
        response_model=SwitchResponse,
        tags=["操作"],
        summary="切换新报文的默认接收队列",
    )
    def switch_queue(body: SwitchRequest) -> dict:
        try:
            previous = engine.switch(body.queue_id)
        except UnknownQueueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "previous_queue": previous,
            "active_queue": body.queue_id,
            "detail": (
                f"新报文改由队列 {body.queue_id} 接收；"
                "各队列里已收下的报文仍由原队列交付"
            ),
            "queues": [engine.queue_view(q) for q in range(engine.n_queues)],
        }

    @app.post(
        "/reset",
        response_model=ResetResponse,
        tags=["运维"],
        summary="清空所有队列、流和交付记录，恢复初始状态",
    )
    def reset() -> ResetResponse:
        engine.reset()
        return ResetResponse()

    return app


app = create_app()
