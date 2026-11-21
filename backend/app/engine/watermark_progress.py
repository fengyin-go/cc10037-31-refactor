"""水位推进与延迟检测引擎。

追踪每条ETL管线的数据分区消费进度：
- 水位推进（基于调度时间的分区递增）
- 延迟检测（当前水位 vs 期望水位）
- 滑动窗口内的迟到数据处理标记

日分区序列的枚举收拢在 app.engine.partition_range 共享边界内；
本引擎只声明进度检查的端点约定：(最近确认水位, 新水位]。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

from app.engine.partition_range import (
    DayPartitionReader,
    InvalidPartitionDate,
    enumerate_day_partitions,
)


class WatermarkStatus(str, Enum):
    NORMAL = "正常"
    DELAYED = "延迟"
    SEVERE = "严重延迟"
    PAUSED = "已暂停"


@dataclass
class WatermarkRecord:
    """一条水位追踪记录。"""
    pipeline_id: str
    partition_field: str                     # 分区字段，如 dt
    current_watermark: str                   # 当前水位，如 2026-11-18
    scheduled_time: datetime
    delay_minutes: int = 0
    last_advance_time: datetime | None = None
    status: WatermarkStatus = WatermarkStatus.NORMAL
    partition_format: str = "%Y-%m-%d"      # 分区格式


@dataclass
class WatermarkAdvanceResult:
    """水位推进结果。"""
    pipeline_id: str
    old_watermark: str
    new_watermark: str
    advanced_partitions: list[str]           # 推进过程中经过的分区
    delay_minutes: int
    new_status: WatermarkStatus


class WatermarkEngine:
    """水位推进与延迟检测引擎。

    用法:
        eng = WatermarkEngine()
        rec = WatermarkRecord(
            pipeline_id="p_dwd_order",
            partition_field="dt",
            current_watermark="2026-11-17",
            scheduled_time=datetime(2026, 11, 18, 8, 0),
        )
        result = eng.advance(rec, "2026-11-18")
        print(result.delay_minutes, result.new_status)

        delay_report = eng.compute_delay_metrics([rec, ...])
    """

    def __init__(self, partition_reader: DayPartitionReader | None = None) -> None:
        # 区间依赖（如分区日历/元数据服务），默认无外部依赖
        self._partition_reader = partition_reader

    def advance(
        self,
        record: WatermarkRecord,
        new_watermark: str,
        *,
        now: datetime | None = None,
    ) -> WatermarkAdvanceResult:
        """将水位推进到 new_watermark，并计算延迟。"""
        now = now or datetime.now()
        old = record.current_watermark

        # 计算通过的分区列表
        partitions = self._enumerate_partitions(
            old, new_watermark, record.partition_format
        )

        # 解析新旧水位为日期
        try:
            new_dt = datetime.strptime(new_watermark, record.partition_format)
        except ValueError:
            new_dt = now

        # 期望水位：基于调度时间+频率
        expected = self._expected_watermark(record.scheduled_time, now)

        # 计算延迟分钟数
        delay = self._compute_delay(
            new_dt, expected
        )

        # 判断状态
        status = WatermarkStatus.NORMAL
        if delay > 120:
            status = WatermarkStatus.SEVERE
        elif delay > 30:
            status = WatermarkStatus.DELAYED

        return WatermarkAdvanceResult(
            pipeline_id=record.pipeline_id,
            old_watermark=old,
            new_watermark=new_watermark,
            advanced_partitions=partitions,
            delay_minutes=delay,
            new_status=status,
        )

    def compute_delay_metrics(
        self,
        records: list[WatermarkRecord],
        *,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """批量计算所有管线的延迟指标。"""
        now = now or datetime.now()
        total = len(records)
        normal = 0
        delayed = 0
        severe = 0
        max_delay = 0
        max_delay_pipeline = ""

        for rec in records:
            try:
                wm_dt = datetime.strptime(rec.current_watermark, rec.partition_format)
            except ValueError:
                wm_dt = now
            expected = self._expected_watermark(rec.scheduled_time, now)
            delay = self._compute_delay(wm_dt, expected)

            if delay > 120:
                severe += 1
            elif delay > 30:
                delayed += 1
            else:
                normal += 1

            if delay > max_delay:
                max_delay = delay
                max_delay_pipeline = rec.pipeline_id

        return {
            "total_pipelines": total,
            "normal": normal,
            "delayed": delayed,
            "severe": severe,
            "max_delay_minutes": max_delay,
            "max_delay_pipeline": max_delay_pipeline,
            "evaluation_time": now.isoformat(),
            "overall_health": "green" if severe == 0 and delayed < total * 0.2 else
                              "yellow" if severe < total * 0.1 else "red",
        }

    def should_trigger_backfill(
        self,
        record: WatermarkRecord,
        backfill_threshold_days: int = 3,
    ) -> tuple[bool, str]:
        """判断是否需要触发回刷补偿。

        如果当前水位落后期望超过 backfill_threshold_days 天，返回 True。
        """
        try:
            wm_dt = datetime.strptime(record.current_watermark, record.partition_format)
        except ValueError:
            return False, "无法解析水位"

        now = datetime.now()
        expected = self._expected_watermark(record.scheduled_time, now)
        delay_days = (expected - wm_dt).days

        if delay_days >= backfill_threshold_days:
            return True, f"落后 {delay_days} 天，建议回刷 {wm_dt.strftime(record.partition_format)} ~ {expected.strftime(record.partition_format)}"
        return False, ""

    # ---- 内部 ----

    def _enumerate_partitions(
        self, start: str, end: str, fmt: str
    ) -> list[str]:
        """枚举 start(不含) 到 end(含) 之间的所有分区值。

        进度检查的端点约定：(最近确认水位, 新水位] —— 从最近确认之后找缺口。
        非法日期保持原响应退化为 [end]；区间依赖读取失败向上抛，
        不消费未枚举完整的序列。
        """
        try:
            sequence = enumerate_day_partitions(
                start, end, fmt,
                include_start=False, include_end=True,
                reader=self._partition_reader,
            )
        except InvalidPartitionDate:
            return [end]
        return sequence.as_list()

    @staticmethod
    def _expected_watermark(scheduled_time: datetime, now: datetime) -> datetime:
        """计算期望水位：当前时间向下取整到天（假设T+1调度）。"""
        # 简化模型：期望水位 = 昨天的日期
        return (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)

    @staticmethod
    def _compute_delay(actual: datetime, expected: datetime) -> int:
        """计算延迟分钟数（期望 vs 实际）。"""
        return max(0, int((expected - actual).total_seconds() / 60))