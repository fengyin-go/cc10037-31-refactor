"""回刷补偿引擎：分区范围计算、并行拆分、进度预估。

回刷（Backfill）是针对历史分区的数据重建操作，
需要将日期范围拆分为可并行执行的子任务批次。

日分区序列的枚举收拢在 app.engine.partition_range 共享边界内；
本引擎只声明回刷规划的端点约定：[开始分区, 结束分区]，
并把边界明确的分区数据切分为调度读取的执行批次。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from app.engine.partition_range import (
    DayPartitionReader,
    InvalidPartitionDate,
    enumerate_day_partitions,
)


class BackfillStatus(str, Enum):
    PENDING = "待执行"
    RUNNING = "执行中"
    COMPLETED = "已完成"
    FAILED = "补偿失败"
    CANCELLED = "已取消"


@dataclass
class PartitionRange:
    """一个分区范围。"""
    start: str
    end: str
    partitions: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.partitions)


@dataclass
class SplitBatch:
    """拆分后的一个并行批次。"""
    batch_id: int
    range: PartitionRange
    status: str = "待执行"
    progress: float = 0.0  # 0~1

    @property
    def partition_count(self) -> int:
        return self.range.count


@dataclass
class BackfillPlan:
    """回刷补偿方案。"""
    pipeline_id: str
    task_name: str
    reason: str
    full_range: PartitionRange
    batches: list[SplitBatch]
    parallelism: int
    total_partitions: int
    estimated_completion: str  # ISO格式
    status: str = "待执行"


class BackfillEngine:
    """回刷补偿引擎。

    用法:
        eng = BackfillEngine()
        plan = eng.plan(
            pipeline_id="p_dwd_order",
            start_partition="2026-11-01",
            end_partition="2026-11-15",
            parallelism=4,
            reason="口径变更：订单金额计算逻辑修正",
        )
        for batch in plan.batches:
            print(f"批次{batch.batch_id}: {batch.range.start}~{batch.range.end}")
    """

    def __init__(self, partition_reader: DayPartitionReader | None = None) -> None:
        # 区间依赖（如分区日历/元数据服务），默认无外部依赖
        self._partition_reader = partition_reader

    def plan(
        self,
        pipeline_id: str,
        start_partition: str,
        end_partition: str,
        parallelism: int = 1,
        reason: str = "",
        partition_format: str = "%Y-%m-%d",
    ) -> BackfillPlan:
        """根据时间范围生成回刷方案。

        Args:
            pipeline_id: 目标管线
            start_partition: 开始分区（含）
            end_partition: 结束分区（含）
            parallelism: 并行度（拆分为多少批次）
            reason: 补偿原因
            partition_format: 分区格式字符串
        """
        # 区间生产收拢到共享边界：回刷约定 [开始分区, 结束分区] 双端皆含。
        # 非法日期保持原降级响应（退化为单分区）；区间依赖读取失败向上抛，
        # 重试成功前不得拿未枚举完整的结果派发批次。
        try:
            sequence = enumerate_day_partitions(
                start_partition, end_partition, partition_format,
                include_start=True, include_end=True,
                reader=self._partition_reader,
            )
        except InvalidPartitionDate:
            # 退化为单分区
            pr = PartitionRange(
                start=start_partition,
                end=end_partition,
                partitions=[start_partition],
            )
            return BackfillPlan(
                pipeline_id=pipeline_id,
                task_name=f"backfill_{pipeline_id}",
                reason=reason,
                full_range=pr,
                batches=[SplitBatch(batch_id=0, range=pr)],
                parallelism=1,
                total_partitions=1,
                estimated_completion=datetime.now().isoformat(),
            )

        all_partitions = sequence.as_list()
        if not all_partitions:
            all_partitions = [start_partition]

        full_range = PartitionRange(
            start=start_partition, end=end_partition, partitions=all_partitions,
        )

        # 拆分批次：尽可能均匀分配
        total = len(all_partitions)
        actual_parallelism = min(parallelism, total)
        base_size = total // actual_parallelism
        remainder = total % actual_parallelism

        batches: list[SplitBatch] = []
        idx = 0
        for batch_id in range(actual_parallelism):
            batch_size = base_size + (1 if batch_id < remainder else 0)
            batch_partitions = all_partitions[idx:idx + batch_size]
            idx += batch_size

            batch_range = PartitionRange(
                start=batch_partitions[0],
                end=batch_partitions[-1],
                partitions=batch_partitions,
            )
            batches.append(SplitBatch(batch_id=batch_id, range=batch_range))

        # 预估完成时间：简单按每分区~5分钟 + 并行加速
        minutes_per_partition = 5
        est_hours = (total * minutes_per_partition) / (actual_parallelism * 60)
        estimated_completion = (datetime.now() + timedelta(hours=est_hours)).isoformat()

        return BackfillPlan(
            pipeline_id=pipeline_id,
            task_name=f"backfill_{pipeline_id}_{start_partition}",
            reason=reason,
            full_range=full_range,
            batches=batches,
            parallelism=actual_parallelism,
            total_partitions=total,
            estimated_completion=estimated_completion,
        )

    def partition_range_info(
        self,
        start_partition: str,
        end_partition: str,
        partition_format: str = "%Y-%m-%d",
    ) -> dict[str, Any]:
        """获取分区范围的详细信息。"""
        try:
            start_dt = datetime.strptime(start_partition, partition_format)
            end_dt = datetime.strptime(end_partition, partition_format)
            days = (end_dt - start_dt).days + 1
            weekends = sum(
                1 for d in range(days)
                if (start_dt + timedelta(days=d)).weekday() >= 5
            )
        except ValueError:
            days = 1
            weekends = 0

        return {
            "start": start_partition,
            "end": end_partition,
            "total_days": days,
            "weekends": weekends,
            "business_days": days - weekends,
            "estimated_data_volume_gb": days * 12.5,  # 假设每天12.5GB
        }

    def split_adaptive(
        self,
        partitions: list[str],
        max_per_batch: int = 10,
    ) -> list[list[str]]:
        """自适应拆分：每个批次最多 max_per_batch 个分区。"""
        result: list[list[str]] = []
        for i in range(0, len(partitions), max_per_batch):
            result.append(partitions[i:i + max_per_batch])
        return result

    def batch_progress_report(
        self,
        plan: BackfillPlan,
        completed_batch_ids: set[int],
    ) -> dict[str, Any]:
        """生成回刷进度报告。"""
        total_batches = len(plan.batches)
        completed = len(completed_batch_ids)
        completed_partitions = sum(
            b.range.count for b in plan.batches if b.batch_id in completed_batch_ids
        )
        return {
            "task_name": plan.task_name,
            "pipeline_id": plan.pipeline_id,
            "total_batches": total_batches,
            "completed_batches": completed,
            "progress_pct": round(completed / max(total_batches, 1) * 100, 1),
            "partitions_done": completed_partitions,
            "partitions_total": plan.total_partitions,
            "data_progress_pct": round(
                completed_partitions / max(plan.total_partitions, 1) * 100, 1
            ),
            "status": "COMPLETED" if completed == total_batches else "IN_PROGRESS",
        }