"""进度检查与回刷规划的日分区范围 —— 重构前后行为对照脚本。

用法:
    python tests/partition_range_parity.py --save /tmp/baseline.json   # 重构前采集基线
    python tests/partition_range_parity.py --check /tmp/baseline.json  # 重构后逐字段比对

覆盖用例（分别交给进度检查与回刷规划）:
    single_day    单日（起止同日）
    cross_month   跨月（2026-11-28 ~ 2026-12-03）
    inverted      倒置（起点晚于终点）
    invalid_start 非法起点日期
    invalid_end   非法终点日期

比对内容：两个入口（WatermarkEngine.advance / BackfillEngine.plan 及
/api/business/backfill/plan）的全部分区日期与响应字段；
estimated_completion 等时间戳字段归一化后比对。
"""
from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine.backfill_plan import BackfillEngine
from app.engine.watermark_progress import WatermarkEngine, WatermarkRecord
from app.routers.business import generate_backfill_plan

FIXED_NOW = datetime(2026, 11, 20, 10, 30, 0)
SCHEDULED = datetime(2026, 11, 19, 8, 0, 0)

CASES = {
    "single_day": ("2026-11-18", "2026-11-18"),
    "cross_month": ("2026-11-28", "2026-12-03"),
    "inverted": ("2026-12-03", "2026-11-28"),
    "invalid_start": ("not-a-date", "2026-12-03"),
    "invalid_end": ("2026-11-28", "bad-date"),
}

VOLATILE_KEYS = {"estimated_completion"}


def normalize(obj):
    """递归归一化时间戳等易变字段。"""
    if isinstance(obj, dict):
        return {
            k: ("<volatile>" if k in VOLATILE_KEYS else normalize(v))
            for k, v in obj.items()
        }
    if isinstance(obj, (list, tuple)):
        return [normalize(v) for v in obj]
    if isinstance(obj, float):
        return round(obj, 6)
    return obj


def capture_progress(start: str, end: str) -> dict:
    """进度检查：从最近确认水位之后找缺口。"""
    engine = WatermarkEngine()
    record = WatermarkRecord(
        pipeline_id="p_dwd_order",
        partition_field="dt",
        current_watermark=start,
        scheduled_time=SCHEDULED,
    )
    result = engine.advance(record, end, now=FIXED_NOW)
    return {
        "old_watermark": result.old_watermark,
        "new_watermark": result.new_watermark,
        "advanced_partitions": result.advanced_partitions,
        "delay_minutes": result.delay_minutes,
        "new_status": result.new_status.value,
    }


def capture_backfill(start: str, end: str) -> dict:
    """回刷规划：按指定起止日切分执行批次。"""
    engine = BackfillEngine()
    plan = engine.plan(
        pipeline_id="p_dwd_order",
        start_partition=start,
        end_partition=end,
        parallelism=4,
        reason="口径变更：订单金额计算逻辑修正",
    )
    return {
        "task_name": plan.task_name,
        "total_partitions": plan.total_partitions,
        "parallelism": plan.parallelism,
        "estimated_completion": plan.estimated_completion,
        "full_range": {
            "start": plan.full_range.start,
            "end": plan.full_range.end,
            "partitions": plan.full_range.partitions,
        },
        "batches": [
            {
                "batch_id": b.batch_id,
                "start": b.range.start,
                "end": b.range.end,
                "partitions": b.range.partitions,
                "status": b.status,
                "progress": b.progress,
            }
            for b in plan.batches
        ],
    }


def capture_backfill_api(start: str, end: str) -> dict:
    """回刷规划入口（API 层）：响应形状与分区日期。"""
    return generate_backfill_plan({
        "pipeline_id": "p_dwd_order",
        "start_partition": start,
        "end_partition": end,
        "parallelism": 4,
        "reason": "口径变更：订单金额计算逻辑修正",
    })


def capture_range_info(start: str, end: str) -> dict:
    return BackfillEngine().partition_range_info(start, end)


def capture_all() -> dict:
    snapshot: dict = {}
    for name, (start, end) in CASES.items():
        snapshot[name] = {
            "progress_check": capture_progress(start, end),
            "backfill_plan": capture_backfill(start, end),
            "backfill_plan_api": capture_backfill_api(start, end),
            "partition_range_info": capture_range_info(start, end),
        }
    return normalize(snapshot)


def main() -> int:
    if len(sys.argv) != 3 or sys.argv[1] not in ("--save", "--check"):
        print(__doc__)
        return 2

    mode, path = sys.argv[1], Path(sys.argv[2])
    current = capture_all()

    if mode == "--save":
        path.write_text(json.dumps(current, ensure_ascii=False, indent=2))
        print(f"基线已保存: {path}")
        return 0

    baseline = json.loads(path.read_text())
    if current == baseline:
        print("PARITY OK: 重构后全部用例的日期与响应与基线完全一致")
        return 0

    print("PARITY FAIL: 以下用例与基线不一致")
    for case_name in CASES:
        old, new = baseline.get(case_name), current.get(case_name)
        if old == new:
            continue
        print(f"\n=== {case_name} ===")
        for section in (old or {}):
            if old[section] != new.get(section):
                print(f"  [{section}]")
                print(f"    基线: {json.dumps(old[section], ensure_ascii=False)}")
                print(f"    当前: {json.dumps(new.get(section), ensure_ascii=False)}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
