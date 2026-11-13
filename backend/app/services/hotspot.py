"""热点分析业务规则。

设计要点（对应需求逐条落地）：

1. 按批次聚合：每个批次绑定一个统计窗口（默认 15 分钟）与一份节点快照，
   窗口内的历史运行记录按节点汇总耗时、吞吐、错误数与错误占比；
2. 窗口纪律：窗口开始时间在分析发起时确定，窗口内新增的节点不在快照里，
   不会被算进上一批次；窗口外的旧运行记录仍然可查询但不参与计算；
3. 并发控制：同一批次同时只允许一次计算，后来的并发请求直接拒绝（409）；
4. 重复分析：同一批次重复分析只保留最新结论，历史版本另表留存可审计；
5. 暂停传播：暂停热点节点时，其全部传递下游节点进入等待，相关采集计划
   显示受影响批次、重试规则写入失败分类、告警清单同步传播范围；
6. 恢复续跑：节点恢复后只从受影响的下游位置继续，已完成区间靠检查点跳过。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from app.seed import (
    DURATION_MS_HOT,
    ERROR_COUNT_HOT,
    ERROR_SHARE_HOT,
    THROUGHPUT_LOW,
    WINDOW_MINUTES,
)
from app.store import store

PIPELINE = "pipeline"
RUNS = "pipeline_run"
ANALYSIS = "hotspot_analysis"
ANALYSIS_HISTORY = "hotspot_analysis_history"
ACTION_LOG = "hotspot_action"
TODO = "hotspot_todo"

ACTIONS = ("暂停", "限流", "观察")
# 暂停场景下下游待办类型；暂停时同时生成一条“待限流”清单。
TODO_WAIT = "待恢复等待"
TODO_THROTTLE = "待限流"


class HotspotConflict(RuntimeError):
    """同一批次已有计算在进行中，或当前状态不允许该处置动作。"""


class HotspotService:
    def __init__(self) -> None:
        # FastAPI 默认在线程池中跑同步路由，用可重入锁保护全部运行态表。
        self._lock = threading.RLock()
        self._running_batches: set[str] = set()
        # 测试钩子：在真正计算前阻塞，方便模拟两个运行并发发起同批次计算。
        self.before_compute: Callable[[str], None] | None = None

    # ------------------------------------------------------------------
    # 基础工具
    # ------------------------------------------------------------------
    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _window_bounds(now: datetime) -> tuple[datetime, datetime]:
        # 滚动窗口：当前时刻向前取 WINDOW_MINUTES 分钟，批次号按整 15 分钟
        # 对齐生成，保证同一值班时段内的重复分析落到同一个批次号上。
        return now - timedelta(minutes=WINDOW_MINUTES), now

    @staticmethod
    def _batch_id_of(now: datetime) -> str:
        minute = (now.minute // WINDOW_MINUTES) * WINDOW_MINUTES
        aligned = now.replace(minute=minute, second=0, microsecond=0)
        return f"BATCH-{aligned.strftime('%Y%m%d%H%M')}"

    @staticmethod
    def _find_node(code: str) -> dict[str, Any] | None:
        for row in store.rows(PIPELINE):
            if row.get("节点编号") == code:
                return row
        return None

    @staticmethod
    def _children_map() -> dict[str, list[str]]:
        children: dict[str, list[str]] = {}
        for row in store.rows(PIPELINE):
            parent = str(row.get("上游节点") or "").strip()
            code = str(row.get("节点编号") or "").strip()
            if parent and parent != "-":
                children.setdefault(parent, []).append(code)
        return children

    @staticmethod
    def _descendants(code: str, children: dict[str, list[str]]) -> list[str]:
        """沿下游边做广度优先遍历，拿到全部传递下游（去重、保序）。"""
        result: list[str] = []
        queue = list(children.get(code, []))
        seen: set[str] = set()
        while queue:
            current = queue.pop(0)
            if current in seen:
                continue
            seen.add(current)
            result.append(current)
            queue.extend(children.get(current, []))
        return result

    @staticmethod
    def _path_to_chain_end(code: str, children: dict[str, list[str]]) -> list[str]:
        """取一条从热点节点到链路末端的代表路径（沿第一条下游边）。"""
        path = [code]
        current = code
        while children.get(current):
            current = children[current][0]
            path.append(current)
        return path

    # ------------------------------------------------------------------
    # 批次分析
    # ------------------------------------------------------------------
    def analyze(self, batch_id: str | None = None) -> dict[str, Any]:
        now = self._now()
        window_start, window_end = self._window_bounds(now)
        batch_id = batch_id or self._batch_id_of(now)

        with self._lock:
            if batch_id in self._running_batches:
                raise HotspotConflict(f"批次 {batch_id} 已有分析在运行，并发请求只接受一次生效")
            self._running_batches.add(batch_id)
        try:
            if self.before_compute is not None:
                self.before_compute(batch_id)
            return self._compute(batch_id, window_start, window_end, now)
        finally:
            with self._lock:
                self._running_batches.discard(batch_id)

    def _compute(
        self,
        batch_id: str,
        window_start: datetime,
        window_end: datetime,
        now: datetime,
    ) -> dict[str, Any]:
        with self._lock:
            # 快照纪律：批次节点集合在首次分析时锁定。重复分析同批次只刷新
            # 窗口指标，统计窗口内新增的节点进不了已存在的批次快照。
            locked_codes = self._snapshot_codes(batch_id)
            if locked_codes is None:
                snapshot = [dict(row) for row in store.rows(PIPELINE)]
            else:
                snapshot = [
                    dict(row) for row in store.rows(PIPELINE)
                    if row.get("节点编号") in locked_codes
                ]
            snapshot_codes = {row["节点编号"] for row in snapshot}
            chain_codes: dict[str, list[str]] = {}
            for row in snapshot:
                chain_codes.setdefault(str(row.get("链路编号") or "未分组"), []).append(
                    str(row["节点编号"])
                )

            window_runs = [
                row for row in store.rows(RUNS)
                if row.get("节点编号") in snapshot_codes
                and window_start <= datetime.fromisoformat(str(row["开始时间"])) < window_end
            ]

            chains: list[dict[str, Any]] = []
            all_hotspots: list[dict[str, Any]] = []
            for chain_id, codes in chain_codes.items():
                chain_runs = [row for row in window_runs if row["节点编号"] in codes]
                chain_total_duration = sum(int(row["耗时毫秒"]) for row in chain_runs)
                chain_total_errors = sum(int(row["错误数"]) for row in chain_runs)

                node_metrics: list[dict[str, Any]] = []
                for code in codes:
                    runs = [row for row in chain_runs if row["节点编号"] == code]
                    samples = len(runs)
                    duration_sum = sum(int(row["耗时毫秒"]) for row in runs)
                    avg_duration = round(duration_sum / samples, 1) if samples else 0.0
                    throughput_avg = (
                        round(sum(int(row["吞吐条分"]) for row in runs) / samples, 1)
                        if samples else 0.0
                    )
                    errors = sum(int(row["错误数"]) for row in runs)
                    node_metrics.append({
                        "节点编号": code,
                        "样本数": samples,
                        "窗口总耗时毫秒": duration_sum,
                        "平均耗时毫秒": avg_duration,
                        "平均吞吐条分": throughput_avg,
                        "窗口错误数": errors,
                        "耗时占比": round(duration_sum / chain_total_duration, 4)
                        if chain_total_duration else 0.0,
                        "错误占比": round(errors / chain_total_errors, 4)
                        if chain_total_errors else 0.0,
                    })

                hotspots = [
                    self._judge(metric) for metric in node_metrics
                    if self._is_hot(metric)
                ]
                hotspots.sort(key=lambda item: item["影响分"], reverse=True)
                for rank, item in enumerate(hotspots, start=1):
                    item["链路内排名"] = rank
                    all_hotspots.append({**item, "链路编号": chain_id})

                chains.append({
                    "链路编号": chain_id,
                    "节点数": len(codes),
                    "窗口样本数": len(chain_runs),
                    "链路窗口总耗时毫秒": chain_total_duration,
                    "链路窗口错误总数": chain_total_errors,
                    "节点指标": node_metrics,
                    "热点节点": hotspots,
                    "无异常": not hotspots,
                    "结论": ("链路运行平稳，暂无热点节点，建议继续观察"
                           if not hotspots
                           else f"识别到 {len(hotspots)} 个热点节点，按影响分排序处理"),
                })

            all_hotspots.sort(key=lambda item: item["影响分"], reverse=True)
            for rank, item in enumerate(all_hotspots, start=1):
                item["全局优先级"] = rank

            result = {
                "id": store.next_id(ANALYSIS),
                "批次编号": batch_id,
                "窗口开始": window_start.isoformat(),
                "窗口结束": window_end.isoformat(),
                "窗口分钟": WINDOW_MINUTES,
                "阈值": {
                    "耗时毫秒": DURATION_MS_HOT,
                    "错误数": ERROR_COUNT_HOT,
                    "错误占比": ERROR_SHARE_HOT,
                    "低吞吐条分": THROUGHPUT_LOW,
                },
                "快照节点": sorted(snapshot_codes),
                "窗口内运行记录数": len(window_runs),
                "链路结论": chains,
                "处理优先级": [
                    {
                        "全局优先级": item["全局优先级"],
                        "链路编号": item["链路编号"],
                        "节点编号": item["节点编号"],
                        "建议动作": item["建议动作"],
                        "影响分": item["影响分"],
                        "受影响下游": item["受影响下游"],
                    }
                    for item in all_hotspots
                ],
                "无异常": not all_hotspots,
                "结论": ("全部链路在统计窗口内无热点，建议继续观察"
                       if not all_hotspots
                       else f"识别到 {len(all_hotspots)} 个热点节点，已按处理优先级排列"),
                "版本": self._next_version(batch_id),
                "分析时间": now.isoformat(),
            }

            # 同批次重复分析：最新结论进主表，旧结论转入历史表。
            existing = self._latest(batch_id)
            if existing is not None:
                store.rows(ANALYSIS_HISTORY).append(dict(existing))
                store.rows(ANALYSIS).remove(existing)
            store.rows(ANALYSIS).append(result)
            return result

    def _next_version(self, batch_id: str) -> int:
        versions = [1]
        for table in (ANALYSIS, ANALYSIS_HISTORY):
            versions.extend(
                int(row["版本"]) + 1 for row in store.rows(table)
                if row.get("批次编号") == batch_id
            )
        return max(versions)

    def _latest(self, batch_id: str) -> dict[str, Any] | None:
        for row in reversed(store.rows(ANALYSIS)):
            if row.get("批次编号") == batch_id:
                return row
        return None

    def _snapshot_codes(self, batch_id: str) -> set[str] | None:
        for table in (ANALYSIS, ANALYSIS_HISTORY):
            for row in store.rows(table):
                if row.get("批次编号") == batch_id:
                    return set(row["快照节点"])
        return None

    def _is_hot(self, metric: dict[str, Any]) -> bool:
        duration_hit = metric["平均耗时毫秒"] >= DURATION_MS_HOT
        error_hit = (
            metric["窗口错误数"] >= ERROR_COUNT_HOT
            and metric["错误占比"] >= ERROR_SHARE_HOT
        )
        throughput_hit = (
            metric["平均吞吐条分"] <= THROUGHPUT_LOW and metric["窗口错误数"] > 0
        )
        return duration_hit or error_hit or throughput_hit

    def _judge(self, metric: dict[str, Any]) -> dict[str, Any]:
        children = self._children_map()
        code = str(metric["节点编号"])
        downstream = self._descendants(code, children)
        path = self._path_to_chain_end(code, children)

        severe = (
            metric["平均耗时毫秒"] >= DURATION_MS_HOT
            and metric["窗口错误数"] >= ERROR_COUNT_HOT
        )
        medium = metric["窗口错误数"] >= ERROR_COUNT_HOT or (
            metric["平均耗时毫秒"] >= DURATION_MS_HOT
        )
        if severe:
            action = "暂停"
            reason = "耗时与错误同时越限，继续运行会放大下游堆积"
        elif medium:
            action = "限流"
            reason = "单一指标越限，先降载保留取数能力"
        else:
            action = "观察"
            reason = "吞吐偏低且伴随错误，暂不处置，列入下批次复核"

        # 影响分：耗时占比与错误占比加权，再叠加下游传播面，分数越高越优先。
        impact = round(
            metric["耗时占比"] * 40
            + metric["错误占比"] * 40
            + min(len(downstream), 5) * 4,
            2,
        )
        return {
            "节点编号": code,
            "平均耗时毫秒": metric["平均耗时毫秒"],
            "平均吞吐条分": metric["平均吞吐条分"],
            "窗口错误数": metric["窗口错误数"],
            "耗时占比": metric["耗时占比"],
            "错误占比": metric["错误占比"],
            "建议动作": action,
            "判定原因": reason,
            "影响分": impact,
            "受影响下游": downstream,
            "受影响路径": path,
        }

    # ------------------------------------------------------------------
    # 处置动作与传播
    # ------------------------------------------------------------------
    def apply_action(
        self,
        analysis_id: int,
        node_code: str,
        action: str,
        *,
        batch_id: str | None = None,
    ) -> dict[str, Any]:
        if action not in ACTIONS:
            raise HotspotConflict(f"动作「{action}」不在暂停、限流、观察范围内")
        with self._lock:
            analysis = store.find(ANALYSIS, analysis_id)
            if analysis is None or (batch_id and analysis["批次编号"] != batch_id):
                raise HotspotConflict("分析结论不存在或批次不匹配，拒绝处置")
            node = self._find_node(node_code)
            if node is None:
                raise HotspotConflict(f"节点 {node_code} 不存在")

            hotspot = next(
                (item for item in analysis["处理优先级"]
                 if item["节点编号"] == node_code),
                None,
            )
            if hotspot is None:
                raise HotspotConflict(f"节点 {node_code} 不是该批次热点，不允许处置")

            bid = str(analysis["批次编号"])
            children = self._children_map()
            downstream = self._descendants(node_code, children)
            affected = [node_code, *downstream]
            log_row: dict[str, Any] = {
                "id": store.next_id(ACTION_LOG),
                "批次编号": bid,
                "节点编号": node_code,
                "动作": action,
                "受影响节点": affected,
                "处理状态": "已执行",
                "操作时间": self._now().isoformat(),
            }

            if action == "暂停":
                self._propagate_pause(node, downstream, bid, analysis)
            elif action == "限流":
                self._propagate_throttle(node, bid, analysis, affected)
            else:
                node["abnormal"] = True
                log_row["处理状态"] = "观察中"

            store.rows(ACTION_LOG).append(log_row)
            return {
                "ok": True,
                "批次编号": bid,
                "动作": action,
                "热点节点": node_code,
                "受影响节点": affected,
                "待办": [row for row in store.rows(TODO)
                        if row["批次编号"] == bid],
            }

    def _propagate_pause(
        self,
        node: dict[str, Any],
        downstream: list[str],
        batch_id: str,
        analysis: dict[str, Any],
    ) -> None:
        node_code = str(node["节点编号"])
        # 热点节点本身暂停；所有传递下游进入等待。
        node["status"] = "已暂停"
        node["节点状态"] = "已暂停"
        node["pending"] = True
        node["abnormal"] = True
        node["暂停批次"] = batch_id
        for code in downstream:
            child = self._find_node(code)
            if child is not None:
                child["status"] = "等待中"
                child["节点状态"] = "等待中"
                child["pending"] = True
                child["等待来源"] = node_code
                child["等待批次"] = batch_id

        # 待办区：每个下游一条“待恢复等待”，热点节点一条“待限流”清单。
        for code in downstream:
            store.rows(TODO).append({
                "id": store.next_id(TODO),
                "批次编号": batch_id,
                "节点编号": code,
                "待办类型": TODO_WAIT,
                "来源节点": node_code,
                "处理状态": "待处理",
                "说明": f"上游 {node_code} 已暂停，节点进入等待，恢复后从断点续跑",
            })
        store.rows(TODO).append({
            "id": store.next_id(TODO),
            "批次编号": batch_id,
            "节点编号": node_code,
            "待办类型": TODO_THROTTLE,
            "来源节点": node_code,
            "处理状态": "待处理",
            "说明": "热点节点暂停期间准备限流参数，恢复后按限流速率放行",
        })

        self._mark_plans(batch_id, analysis)
        self._classify_retry(node_code, batch_id, analysis)
        self._sync_alert(node_code, downstream, batch_id, analysis, "待恢复")

    def _propagate_throttle(
        self,
        node: dict[str, Any],
        batch_id: str,
        analysis: dict[str, Any],
        affected: list[str],
    ) -> None:
        node_code = str(node["节点编号"])
        node["abnormal"] = True
        node["限流批次"] = batch_id
        store.rows(TODO).append({
            "id": store.next_id(TODO),
            "批次编号": batch_id,
            "节点编号": node_code,
            "待办类型": TODO_THROTTLE,
            "来源节点": node_code,
            "处理状态": "待处理",
            "说明": "热点节点执行限流，等待速率回落确认后关闭",
        })
        self._classify_retry(node_code, batch_id, analysis)
        self._sync_alert(node_code, affected[1:], batch_id, analysis, "限流观察")
        self._mark_plans(batch_id, analysis)

    def _mark_plans(self, batch_id: str, analysis: dict[str, Any]) -> None:
        # 直接从批次结论里取存在热点的受影响链路。
        affected_chains = {
            chain["链路编号"]
            for chain in analysis["链路结论"] if not chain["无异常"]
        }
        for plan in store.rows("taskplan"):
            if plan.get("链路编号") in affected_chains:
                plan["受影响批次"] = batch_id
                plan["计划状态"] = "受热点影响"
                plan["pending"] = True

    def _classify_retry(
        self,
        node_code: str,
        batch_id: str,
        analysis: dict[str, Any],
    ) -> None:
        chain_id = next(
            node.get("链路编号") for node in store.rows(PIPELINE)
            if node.get("节点编号") == node_code
        )
        metric = next(
            item for chain in analysis["链路结论"]
            if chain["链路编号"] == chain_id
            for item in chain["节点指标"]
            if item["节点编号"] == node_code
        )
        if metric["平均耗时毫秒"] >= DURATION_MS_HOT:
            classification = f"热点超时/高耗时（批次 {batch_id}，均值 {metric['平均耗时毫秒']}ms）"
        else:
            classification = f"热点错误堆积（批次 {batch_id}，窗口错误 {metric['窗口错误数']}）"
        for rule in store.rows("retry"):
            if rule.get("链路编号") == chain_id:
                rule["失败分类"] = classification
                rule["关联批次"] = batch_id
                rule["abnormal"] = True

    def _sync_alert(
        self,
        node_code: str,
        downstream: list[str],
        batch_id: str,
        analysis: dict[str, Any],
        handle_state: str,
    ) -> None:
        chain_id = next(
            node.get("链路编号") for node in store.rows(PIPELINE)
            if node.get("节点编号") == node_code
        )
        store.rows("alert").append({
            "id": store.next_id("alert"),
            "status": "待校验",
            "pending": True,
            "abnormal": True,
            "规则编号": f"ALER-HOT-{batch_id}-{node_code}",
            "监控指标": f"链路热点传播（{node_code}）",
            "告警级别": "严重" if handle_state == "待恢复" else "警告",
            "触发条件": "热点分析处置触发，传播范围与处置批次绑定",
            "通知渠道": "值班群、短信",
            "静默时间": self._now().strftime("%Y-%m-%d %H:%M"),
            "升级策略": "30 分钟未恢复升级主管",
            "是否启用": "是",
            "来源": "热点分析同步",
            "影响节点": "、".join([node_code, *downstream]),
            "处理状态": handle_state,
            "关联批次": batch_id,
            "链路编号": chain_id,
        })

    # ------------------------------------------------------------------
    # 恢复与续跑
    # ------------------------------------------------------------------
    def resume(self, node_code: str) -> dict[str, Any]:
        with self._lock:
            node = self._find_node(node_code)
            if node is None:
                raise HotspotConflict(f"节点 {node_code} 不存在")
            if node.get("status") != "已暂停":
                raise HotspotConflict(f"节点 {node_code} 未处于暂停状态，无需恢复")

            batch_id = str(node.get("暂停批次") or "")
            children = self._children_map()
            downstream = self._descendants(node_code, children)

            # 检查点续跑：已完成区间原样保留，只从下游断点之后继续。
            resume_from = node.get("已完成区间", "0-0")
            node["status"] = "运行中"
            node["节点状态"] = "运行中"
            node["abnormal"] = False
            node.pop("暂停批次", None)
            node["续跑起点"] = resume_from

            resumed: list[dict[str, Any]] = [{
                "节点编号": node_code,
                "续跑起点": resume_from,
                "说明": "已完成区间不重跑，从断点之后继续",
            }]
            for code in downstream:
                child = self._find_node(code)
                if child is not None and child.get("status") == "等待中":
                    checkpoint = child.get("已完成区间", "0-0")
                    child["status"] = "运行中"
                    child["节点状态"] = "运行中"
                    child.pop("等待来源", None)
                    child.pop("等待批次", None)
                    child["续跑起点"] = checkpoint
                    resumed.append({
                        "节点编号": code,
                        "续跑起点": checkpoint,
                        "说明": f"等待解除，自检查点 {checkpoint} 续跑",
                    })

            # 待办关闭、告警保留影响节点并标记恢复、计划解除批次影响。
            for todo in store.rows(TODO):
                if todo.get("批次编号") == batch_id and todo.get("来源节点") == node_code:
                    todo["处理状态"] = "已完成"
            for alert in store.rows("alert"):
                if alert.get("关联批次") == batch_id and node_code in str(
                    alert.get("影响节点", "")
                ):
                    alert["处理状态"] = "已恢复"
                    alert["pending"] = False
            for plan in store.rows("taskplan"):
                if plan.get("受影响批次") == batch_id:
                    plan["受影响批次"] = ""
                    plan["计划状态"] = "运行中"
            store.rows(ACTION_LOG).append({
                "id": store.next_id(ACTION_LOG),
                "批次编号": batch_id,
                "节点编号": node_code,
                "动作": "恢复",
                "受影响节点": [node_code, *downstream],
                "处理状态": "已恢复",
                "操作时间": self._now().isoformat(),
            })
            return {
                "ok": True,
                "批次编号": batch_id,
                "恢复节点": node_code,
                "续跑明细": resumed,
                "未重跑区间": {item["节点编号"]: item["续跑起点"] for item in resumed},
            }

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def list_analyses(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = store.rows(ANALYSIS)
            if batch_id:
                rows = [row for row in rows if row.get("批次编号") == batch_id]
            return [dict(row) for row in rows]

    def list_todos(self, todo_type: str | None = None) -> list[dict[str, Any]]:
        with self._lock:
            rows = store.rows(TODO)
            if todo_type:
                rows = [row for row in rows if row.get("待办类型") == todo_type]
            return [dict(row) for row in rows]

    def list_runs(self, node_code: str | None = None) -> list[dict[str, Any]]:
        """历史运行记录始终可查询，含窗口外旧记录。"""
        with self._lock:
            rows = store.rows(RUNS)
            if node_code:
                rows = [row for row in rows if row.get("节点编号") == node_code]
            return [dict(row) for row in rows]

    def config(self) -> dict[str, Any]:
        return {
            "窗口分钟": WINDOW_MINUTES,
            "阈值": {
                "平均耗时毫秒": DURATION_MS_HOT,
                "窗口错误数": ERROR_COUNT_HOT,
                "错误占比": ERROR_SHARE_HOT,
                "低吞吐条分": THROUGHPUT_LOW,
            },
            "动作集合": list(ACTIONS),
            "待办类型": [TODO_WAIT, TODO_THROTTLE],
        }


hotspot_service = HotspotService()
