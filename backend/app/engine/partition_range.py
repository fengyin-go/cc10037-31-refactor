"""日分区序列的计算边界。

进度检查与回刷规划共用同一套 "起止日期 → 日分区序列" 的枚举能力，
但两者的端点约定不同：

- 进度检查：(最近确认水位, 新水位] —— 从最近确认之后找缺口
- 回刷规划：[开始分区, 结束分区] —— 按指定起止日切分执行批次

本模块只承担"区间生产"职责，并把它收拢在一次可完整提交的调用内：

1. 原子产出：完整序列在局部缓冲中逐日构建、逐日经区间依赖确认，
   全部就绪后才作为不可变结果返回；任何失败都抛错，
   调用方永远拿不到枚举到一半的中间结果 —— 因此由区间计算本身
   保证一次得出可消费的日期，规划侧不存在"接手未完成结果、
   确认后再交给调度"的补偿通道。
2. 端点显式：起止端点含/不含由调用方按各自约定声明，
   本模块不预设任何一侧的日期含义。
3. 失败分类：非法日期（InvalidPartitionDate）由调用方按各自
   约定降级；区间依赖读取失败（PartitionDependencyError）可重试，
   重试成功前调用方不得拿未枚举完整的结果派发批次。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Protocol


class PartitionRangeError(Exception):
    """日分区区间无法一次完整产出的基类。"""


class InvalidPartitionDate(PartitionRangeError, ValueError):
    """端点日期无法按分区格式解析。

    同时是 ValueError，与既有 except ValueError 处理路径保持兼容。
    """


class PartitionDependencyError(PartitionRangeError):
    """区间依赖读取失败（如分区日历/元数据服务），可重试。"""


class DayPartitionReader(Protocol):
    """区间依赖：逐日确认分区数据可读。"""

    def ensure_readable(self, day: date) -> None:
        """确认某日分区可读；不可读时抛 PartitionDependencyError。"""
        ...


@dataclass(frozen=True)
class DayPartitionSequence:
    """一次完整枚举得出的日分区序列：边界明确、可立即消费。"""

    start: str                    # 调用方给出的原始起点
    end: str                      # 调用方给出的原始终点
    include_start: bool           # 起点是否计入序列
    include_end: bool             # 终点是否计入序列
    partitions: tuple[str, ...]   # 完整分区序列（区间为空时为空元组）

    def as_list(self) -> list[str]:
        """以列表形式取出完整分区序列。"""
        return list(self.partitions)


def parse_day_partition(value: str, fmt: str) -> datetime:
    """把分区字符串解析为日期时间；非法日期抛 InvalidPartitionDate。"""
    try:
        return datetime.strptime(value, fmt)
    except ValueError as exc:
        raise InvalidPartitionDate(
            f"非法分区日期: {value!r} 不符合格式 {fmt!r}"
        ) from exc


def enumerate_day_partitions(
    start: str,
    end: str,
    fmt: str = "%Y-%m-%d",
    *,
    include_start: bool,
    include_end: bool,
    reader: DayPartitionReader | None = None,
) -> DayPartitionSequence:
    """枚举 start~end 的日分区序列，端点含/不含由参数显式指定。

    原子性：序列先在局部缓冲中构建完整、逐日经 reader 确认后才返回。
    端点非法抛 InvalidPartitionDate；区间依赖读取失败抛
    PartitionDependencyError —— 两种失败下调用方都拿不到半截序列，
    重试需重新调用本函数整体重算。
    """
    start_dt = parse_day_partition(start, fmt)
    end_dt = parse_day_partition(end, fmt)

    first = start_dt if include_start else start_dt + timedelta(days=1)
    last = end_dt if include_end else end_dt - timedelta(days=1)

    buffer: list[str] = []
    day = first
    while day <= last:
        if reader is not None:
            reader.ensure_readable(day.date())
        buffer.append(day.strftime(fmt))
        day += timedelta(days=1)

    return DayPartitionSequence(
        start=start,
        end=end,
        include_start=include_start,
        include_end=include_end,
        partitions=tuple(buffer),
    )
