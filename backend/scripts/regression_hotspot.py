"""热点分析验收回归：通过 HTTP 接口验证 PIPE-0002（20 个错误）与两个下游节点
在暂停后，链路列表、采集计划、重试规则、告警清单呈现同一传播范围。

运行：cd backend && .venv/bin/python scripts/regression_hotspot.py
"""
from __future__ import annotations

import json
import urllib.request

BASE = "http://127.0.0.1:8000"


def call(method: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE + path, data=data, method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read())


def assert_true(cond: bool, text: str) -> None:
    if not cond:
        raise AssertionError(text)
    print("  PASS:", text)


def main() -> None:
    print("== 1. 发起批次分析 ==")
    code, analysis = call("POST", "/api/hotspot/analyses", {})
    assert_true(code == 200, f"分析接口 200（实际 {code}）")
    bid = analysis["批次编号"]

    main_chain = next(c for c in analysis["链路结论"] if c["链路编号"] == "LINK-MAIN")
    m2 = next(m for m in main_chain["节点指标"] if m["节点编号"] == "PIPE-0002")
    assert_true(m2["窗口错误数"] == 20, "PIPE-0002 窗口错误数为 20")
    assert_true(m2["平均耗时毫秒"] == 860.0, "PIPE-0002 平均耗时 860ms")
    top = analysis["处理优先级"][0]
    assert_true(top["节点编号"] == "PIPE-0002", "PIPE-0002 为全局第一优先级")
    assert_true(top["建议动作"] == "暂停", "处置建议为暂停")
    assert_true(top["受影响下游"] == ["PIPE-0003", "PIPE-0004"], "受影响下游恰为两个节点")

    print("== 2. 窗口外旧记录不参与计算 ==")
    _, runs = call("GET", "/api/hotspot/runs?node_code=PIPE-0002")
    outside = [r for r in runs["items"] if not r["窗口内"]]
    assert_true(len(outside) == 1 and outside[0]["错误数"] == 99, "窗口外记录可查询但不计入批次")

    print("== 3. 暂停热点节点 ==")
    code, result = call("POST", "/api/hotspot/actions",
                        {"node_code": "PIPE-0002", "action": "暂停", "batch_id": bid})
    assert_true(code == 200 and result["ok"], "暂停动作生效")
    scope = result["entry"]["受影响节点"]
    assert_true(scope == ["PIPE-0002", "PIPE-0003", "PIPE-0004"], f"传播范围 {scope}")

    print("== 4. 三处清单呈现同一传播范围 ==")
    _, pipes = call("GET", "/api/pipeline")
    states = {r["节点编号"]: r["节点状态"] for r in pipes["items"]}
    assert_true(states["PIPE-0002"] == "已暂停", "链路列表：热点节点已暂停")
    assert_true(states["PIPE-0003"] == "等待中" and states["PIPE-0004"] == "等待中",
                "链路列表：两个下游节点进入等待")
    assert_true(states["PIPE-0001"] == "运行中" and states["PIPE-0005"] == "运行中",
                "链路列表：范围外节点保持运行")

    _, plans = call("GET", "/api/taskplan")
    affected_plans = {p["计划编号"]: p for p in plans["items"] if p["受影响批次"] == bid}
    assert_true(set(affected_plans) == {"TASK-0001", "TASK-0002"},
                "采集计划：同链路两个计划显示受影响批次")
    assert_true(all(p["计划状态"] == "受热点影响" for p in affected_plans.values()),
                "采集计划：状态标记为受热点影响")

    _, retries = call("GET", "/api/retry")
    linked = [r for r in retries["items"] if r["关联批次"] == bid]
    assert_true({r["规则编号"] for r in linked} == {"RETR-0001", "RETR-0002"},
                "重试规则：同链路两条规则写入失败分类与批次")
    assert_true(all("热点" in r["失败分类"] for r in linked), "失败分类内容正确")

    _, alerts = call("GET", "/api/alert")
    hot_alerts = [a for a in alerts["items"] if a.get("来源") == "热点分析同步"]
    assert_true(len(hot_alerts) == 1, "告警清单：同步一条传播告警")
    assert_true(hot_alerts[0]["影响节点"] == "PIPE-0002、PIPE-0003、PIPE-0004",
                "告警清单：保留受影响节点且范围一致")

    print("== 5. 待办区：下游等待清单 + 待限流清单 ==")
    _, todos = call("GET", "/api/hotspot/todos")
    wait_nodes = {t["节点编号"] for t in todos["items"] if t["待办类型"] == "待恢复等待"}
    throttle_nodes = {t["节点编号"] for t in todos["items"] if t["待办类型"] == "待限流"}
    assert_true(wait_nodes == {"PIPE-0003", "PIPE-0004"}, "待恢复等待清单覆盖两个下游")
    assert_true(throttle_nodes == {"PIPE-0002"}, "待限流清单新增热点节点一条")

    print("== 6. 恢复后只从受影响下游位置续跑 ==")
    code, resumed = call("POST", "/api/hotspot/resume", {"node_code": "PIPE-0002"})
    assert_true(code == 200 and resumed["ok"], "恢复接口生效")
    checkpoints = resumed["entry"]["未重跑区间"]
    assert_true(checkpoints == {"PIPE-0002": "0-1800", "PIPE-0003": "0-1800", "PIPE-0004": "0-1750"},
                f"已完成区间不重跑：{checkpoints}")
    _, pipes2 = call("GET", "/api/pipeline")
    states2 = {r["节点编号"]: r["节点状态"] for r in pipes2["items"]}
    assert_true(all(s == "运行中" for s in states2.values()), "全部节点恢复运行")
    _, alerts2 = call("GET", "/api/alert")
    kept = next(a for a in alerts2["items"] if a.get("来源") == "热点分析同步")
    assert_true(kept["处理状态"] == "已恢复" and kept["影响节点"], "告警保留影响节点并标记已恢复")

    print("\n全部验收项通过 ✅")


if __name__ == "__main__":
    main()
