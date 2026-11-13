"""热点分析接口。

覆盖：窗口阈值配置、发起批次分析、读取结论与处理优先级、暂停/限流/观察处置、
恢复续跑、传播待办（含待限流清单）与历史运行记录查询。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.schemas import ActionResult
from app.services.hotspot import HotspotConflict, hotspot_service

router = APIRouter(prefix="/api/hotspot", tags=["热点分析"])


class AnalyzePayload(BaseModel):
    batch_id: str | None = None


class ActionPayload(BaseModel):
    node_code: str
    action: str
    batch_id: str | None = None


class ResumePayload(BaseModel):
    node_code: str


@router.get("/config")
def get_config() -> dict[str, Any]:
    """返回统计窗口与热点阈值，前端据此展示判定口径。"""
    return hotspot_service.config()


@router.post("/analyses")
def create_analysis(payload: AnalyzePayload) -> dict[str, Any]:
    """发起一个批次的热点计算；同批次并发只接受一次，重复请求返回 409。"""
    try:
        return hotspot_service.analyze(payload.batch_id)
    except HotspotConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.get("/analyses")
def list_analyses(
    batch_id: str | None = Query(default=None, description="按批次编号过滤"),
) -> dict[str, Any]:
    """热点结论列表：同一批次只保留最新一条，历史版本在 history 接口。"""
    items = hotspot_service.list_analyses(batch_id)
    return {"items": items, "total": len(items)}


@router.get("/analyses/history")
def list_history() -> dict[str, Any]:
    """重复分析产生的历史版本，保留可审计轨迹。"""
    from app.store import store

    rows = store.rows("hotspot_analysis_history")
    return {"items": rows, "total": len(rows)}


@router.post("/actions")
def apply_action(payload: ActionPayload) -> ActionResult:
    """对热点节点执行暂停、限流或观察，并把传播范围写入下游各模块。"""
    try:
        result = hotspot_service.apply_action(
            analysis_id=_latest_analysis_id(payload.batch_id),
            node_code=payload.node_code,
            action=payload.action,
            batch_id=payload.batch_id,
        )
    except HotspotConflict as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(
        ok=True,
        message=f"节点 {payload.node_code} 已执行{payload.action}，传播 {len(result['受影响节点'])} 个节点",
        entry=result,
    )


@router.post("/resume")
def resume_node(payload: ResumePayload) -> ActionResult:
    """恢复暂停节点：下游解除等待，只从检查点续跑，不重跑已完成区间。"""
    try:
        result = hotspot_service.resume(payload.node_code)
    except HotspotConflict as exc:
        return ActionResult(ok=False, message=str(exc))
    return ActionResult(
        ok=True,
        message=f"节点 {payload.node_code} 已恢复，{len(result['续跑明细'])} 个节点从断点续跑",
        entry=result,
    )


@router.get("/todos")
def list_todos(
    todo_type: str | None = Query(default=None, description="待恢复等待 / 待限流"),
) -> dict[str, Any]:
    """传播待办清单，支持按待办类型过滤，待限流清单单独可取。"""
    items = hotspot_service.list_todos(todo_type)
    return {"items": items, "total": len(items)}


@router.get("/runs")
def list_runs(
    node_code: str | None = Query(default=None, description="按节点编号过滤历史运行记录"),
) -> dict[str, Any]:
    """链路节点历史运行记录，窗口外的旧记录同样可查询。"""
    items = hotspot_service.list_runs(node_code)
    return {"items": items, "total": len(items)}


def _latest_analysis_id(batch_id: str | None) -> int:
    analyses = hotspot_service.list_analyses(batch_id)
    if not analyses:
        raise HotspotConflict("尚未生成任何热点分析结论，请先发起批次分析")
    return int(analyses[-1]["id"])
