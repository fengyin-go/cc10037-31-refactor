"""跨月枚举中途失败的原子性验证。

场景：区间依赖（分区日历/元数据服务）在跨月枚举到月初（如 2026-12-01）
时读取失败。验证：

1. 共享边界：枚举要么一次产出完整可消费的序列，要么抛
   PartitionDependencyError，调用方拿不到半截结果。
2. 回刷规划：依赖失败时 plan 整体失败、不派发任何批次
   （不会退化成单分区计划混入队列）；依赖恢复后重试，
   调度读取到的批次分区日期是完整的跨月序列。
3. 进度检查：依赖失败时 advance 整体失败（不会退化成 [end]），
   重试后得到完整缺口分区。
4. 非法日期响应不受依赖失败影响，两个调用方各自保持原约定。
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine.backfill_plan import BackfillEngine
from app.engine.partition_range import (
    InvalidPartitionDate,
    PartitionDependencyError,
    enumerate_day_partitions,
)
from app.engine.watermark_progress import WatermarkEngine, WatermarkRecord

CROSS_START = "2026-11-28"
CROSS_END = "2026-12-03"
EXPECTED_FULL = [
    "2026-11-28", "2026-11-29", "2026-11-30",
    "2026-12-01", "2026-12-02", "2026-12-03",
]
EXPECTED_GAP = EXPECTED_FULL[1:]  # 进度检查不含起点


class FlakyMonthStartReader:
    """在月初（day == 1）读取失败的区间依赖，可恢复。"""

    def __init__(self) -> None:
        self.healthy = False
        self.attempts: list[date] = []

    def ensure_readable(self, day: date) -> None:
        self.attempts.append(day)
        if not self.healthy and day.day == 1:
            raise PartitionDependencyError(f"分区日历服务读取失败: {day}")


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)
    print(f"  OK  {message}")


def test_shared_boundary_atomic() -> None:
    print("[1] 共享边界：跨月枚举在月初失败时不产出半截序列")
    reader = FlakyMonthStartReader()
    try:
        enumerate_day_partitions(
            CROSS_START, CROSS_END,
            include_start=True, include_end=True, reader=reader,
        )
    except PartitionDependencyError as exc:
        check(True, f"枚举在月初中断并抛 PartitionDependencyError: {exc}")
    else:
        raise AssertionError("依赖失败时枚举竟然成功返回")
    check(
        reader.attempts[-1] == date(2026, 12, 1),
        f"中断点正是月初 2026-12-01（实际 {reader.attempts[-1]}）",
    )

    reader.healthy = True
    seq = enumerate_day_partitions(
        CROSS_START, CROSS_END,
        include_start=True, include_end=True, reader=reader,
    )
    check(
        seq.as_list() == EXPECTED_FULL,
        f"依赖恢复后重试一次得出完整序列 {seq.as_list()}",
    )


def test_backfill_never_dispatches_partial() -> None:
    print("[2] 回刷规划：依赖失败不派发批次，重试后批次分区完整")
    reader = FlakyMonthStartReader()
    engine = BackfillEngine(partition_reader=reader)

    try:
        engine.plan(
            pipeline_id="p_dwd_order",
            start_partition=CROSS_START,
            end_partition=CROSS_END,
            parallelism=4,
            reason="跨月回刷",
        )
    except PartitionDependencyError:
        check(True, "依赖失败时 plan 整体抛错，队列拿不到任何批次")
    else:
        raise AssertionError("依赖失败时 plan 竟然返回了计划（可能派发了不完整批次）")

    reader.healthy = True
    plan = engine.plan(
        pipeline_id="p_dwd_order",
        start_partition=CROSS_START,
        end_partition=CROSS_END,
        parallelism=4,
        reason="跨月回刷",
    )
    dispatched = [p for b in plan.batches for p in b.range.partitions]
    check(
        dispatched == EXPECTED_FULL,
        f"重试后调度读取到的批次分区日期完整: {dispatched}",
    )
    for batch in plan.batches:
        check(
            batch.range.start == batch.range.partitions[0]
            and batch.range.end == batch.range.partitions[-1],
            f"批次{batch.batch_id} 边界明确: "
            f"{batch.range.start} ~ {batch.range.end} ({len(batch.range.partitions)}个分区)",
        )


def test_progress_never_consumes_partial() -> None:
    print("[3] 进度检查：依赖失败不退化为 [end]，重试后缺口完整")
    reader = FlakyMonthStartReader()
    engine = WatermarkEngine(partition_reader=reader)
    record = WatermarkRecord(
        pipeline_id="p_dwd_order",
        partition_field="dt",
        current_watermark=CROSS_START,
        scheduled_time=datetime(2026, 12, 4, 8, 0),
    )

    try:
        engine.advance(record, CROSS_END, now=datetime(2026, 12, 4, 10, 0))
    except PartitionDependencyError:
        check(True, "依赖失败时 advance 整体抛错，不消费半截缺口序列")
    else:
        raise AssertionError("依赖失败时 advance 竟然返回（可能退化为 [end]）")

    reader.healthy = True
    result = engine.advance(record, CROSS_END, now=datetime(2026, 12, 4, 10, 0))
    check(
        result.advanced_partitions == EXPECTED_GAP,
        f"重试后缺口分区完整: {result.advanced_partitions}",
    )


def test_invalid_date_responses_intact() -> None:
    print("[4] 非法日期响应不受依赖机制影响")
    reader = FlakyMonthStartReader()  # 依赖仍不健康
    backfill = BackfillEngine(partition_reader=reader)
    plan = backfill.plan(
        pipeline_id="p_dwd_order",
        start_partition="bad-date",
        end_partition=CROSS_END,
        parallelism=4,
    )
    check(
        plan.total_partitions == 1 and plan.full_range.partitions == ["bad-date"],
        "回刷非法日期仍退化为单分区计划",
    )

    progress = WatermarkEngine(partition_reader=reader)
    record = WatermarkRecord(
        pipeline_id="p_dwd_order",
        partition_field="dt",
        current_watermark="bad-date",
        scheduled_time=datetime(2026, 12, 4, 8, 0),
    )
    result = progress.advance(record, CROSS_END, now=datetime(2026, 12, 4, 10, 0))
    check(
        result.advanced_partitions == [CROSS_END],
        f"进度非法日期仍退化为 [end]: {result.advanced_partitions}",
    )

    check(
        issubclass(InvalidPartitionDate, ValueError),
        "InvalidPartitionDate 兼容既有 except ValueError 路径",
    )


if __name__ == "__main__":
    test_shared_boundary_atomic()
    test_backfill_never_dispatches_partial()
    test_progress_never_consumes_partial()
    test_invalid_date_responses_intact()
    print("\nFAILURE-INJECTION OK: 跨月枚举失败场景全部符合原子性约定")
