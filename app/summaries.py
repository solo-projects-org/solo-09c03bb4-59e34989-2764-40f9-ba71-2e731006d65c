"""多级摘要的构建与查询。

摘要块层级：
- level 1：每 BLOCK_SIZE 个原始采样聚合出的最细索引块
- level k>1：由 level k-1 中每 BLOCK_SIZE 个子块合并而成的更粗块

每个块保存：首尾点、压力最高/最低点（并列取最早）、采样点数、
覆盖的原始 rowid 区间与时间区间（end_ts 为最后一点的时间戳）。

查询时优先使用“完整落在查询区间内的最粗块”，只有区间边界切开
level 1 块时，才回退读取该块内的原始采样。
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Optional

from . import config

# blocks 表的列 -> Stats 字段
_BLOCK_FIELDS = (
    "sample_count",
    "first_ts",
    "last_ts",
    "first_pressure",
    "last_pressure",
    "max_pressure",
    "max_pressure_ts",
    "min_pressure",
    "min_pressure_ts",
)


@dataclass
class Stats:
    """一段采样（原始行或摘要块）的四点统计结果。"""

    sample_count: int = 0
    first_ts: Optional[int] = None
    last_ts: Optional[int] = None
    first_pressure: Optional[float] = None
    last_pressure: Optional[float] = None
    max_pressure: Optional[float] = None
    max_pressure_ts: Optional[int] = None
    min_pressure: Optional[float] = None
    min_pressure_ts: Optional[int] = None

    @classmethod
    def from_row(cls, row) -> "Stats":
        return cls(**{f: row[f] for f in _BLOCK_FIELDS})

    def to_tuple(self) -> tuple:
        return tuple(getattr(self, f) for f in _BLOCK_FIELDS)


def stats_from_sample(ts: int, pressure: float) -> Stats:
    """单点统计。"""
    return Stats(
        sample_count=1,
        first_ts=ts,
        last_ts=ts,
        first_pressure=pressure,
        last_pressure=pressure,
        max_pressure=pressure,
        max_pressure_ts=ts,
        min_pressure=pressure,
        min_pressure_ts=ts,
    )


def merge_stats(a: Stats, b: Stats) -> Stats:
    """按时序合并两段统计。极值并列时保留时间戳更早的一个。"""
    if a.sample_count == 0:
        return b
    if b.sample_count == 0:
        return a
    if b.max_pressure > a.max_pressure or (
        b.max_pressure == a.max_pressure and b.max_pressure_ts < a.max_pressure_ts
    ):
        max_p, max_ts = b.max_pressure, b.max_pressure_ts
    else:
        max_p, max_ts = a.max_pressure, a.max_pressure_ts
    if b.min_pressure < a.min_pressure or (
        b.min_pressure == a.min_pressure and b.min_pressure_ts < a.min_pressure_ts
    ):
        min_p, min_ts = b.min_pressure, b.min_pressure_ts
    else:
        min_p, min_ts = a.min_pressure, a.min_pressure_ts
    return Stats(
        sample_count=a.sample_count + b.sample_count,
        first_ts=a.first_ts,
        last_ts=b.last_ts,
        first_pressure=a.first_pressure,
        last_pressure=b.last_pressure,
        max_pressure=max_p,
        max_pressure_ts=max_ts,
        min_pressure=min_p,
        min_pressure_ts=min_ts,
    )


_INSERT_BLOCK_SQL = f"""INSERT INTO blocks (
        test_id, level, block_index,
        start_ts, end_ts,
        {", ".join(_BLOCK_FIELDS)},
        first_rowid, last_rowid
    ) VALUES (
        ?, ?, ?, ?, ?,
        {", ".join("?" for _ in _BLOCK_FIELDS)},
        ?, ?
    )"""


def _insert_block(
    conn: sqlite3.Connection,
    test_id: int,
    level: int,
    block_index: int,
    entry: tuple,
) -> None:
    st, first_rid, last_rid = entry
    conn.execute(
        _INSERT_BLOCK_SQL,
        (
            test_id,
            level,
            block_index,
            st.first_ts,
            st.last_ts,
            *st.to_tuple(),
            first_rid,
            last_rid,
        ),
    )


def _iter_level1_blocks(
    conn: sqlite3.Connection,
    test_id: int,
    block_size: int,
    first_rowid: int,
    last_rowid: int,
):
    """逐块扫描原始采样，写入并产出 level 1 摘要块。

    每次只取最多 block_size 行，因此内存不随试验采样数增长。块写完后才
    产出，供更高层生成器按 block_size 个一组消费。
    """
    next_rowid = first_rowid
    index = 0
    while next_rowid <= last_rowid:
        rows = conn.execute(
            """SELECT rowid AS rid, ts, pressure FROM samples NOT INDEXED
               WHERE test_id = ? AND rowid >= ? AND rowid <= ?
               ORDER BY rowid LIMIT ?""",
            (test_id, next_rowid, last_rowid, block_size),
        ).fetchall()
        if not rows:
            break
        acc = Stats()
        for row in rows:
            acc = merge_stats(acc, stats_from_sample(row["ts"], row["pressure"]))
        entry = (acc, rows[0]["rid"], rows[-1]["rid"])
        _insert_block(conn, test_id, 1, index, entry)
        yield entry
        index += 1
        next_rowid = rows[-1]["rid"] + 1


def _iter_merged_blocks(
    conn: sqlite3.Connection,
    test_id: int,
    level: int,
    block_size: int,
    children,
):
    """把下一级块按 block_size 个一组流式合并，写入并产出本级块。"""
    acc = Stats()
    first_rid: Optional[int] = None
    last_rid: Optional[int] = None
    count = 0
    index = 0
    for child_stats, child_first_rid, child_last_rid in children:
        if first_rid is None:
            first_rid = child_first_rid
        acc = merge_stats(acc, child_stats)
        last_rid = child_last_rid
        count += 1
        if count == block_size:
            entry = (acc, first_rid, last_rid)
            _insert_block(conn, test_id, level, index, entry)
            yield entry
            index += 1
            acc = Stats()
            first_rid = None
            last_rid = None
            count = 0
    if count:
        entry = (acc, first_rid, last_rid)
        _insert_block(conn, test_id, level, index, entry)
        yield entry


def build_summaries(
    conn: sqlite3.Connection,
    test_id: int,
    block_size: int,
    first_rowid: int | None = None,
    last_rowid: int | None = None,
    sample_count: int | None = None,
) -> int:
    """为已写入 samples 的试验构建全部摘要层级，返回最高 level。

    调用方负责事务管理。采样按块流式扫描，层与层之间通过生成器逐块合并，
    内存只保留当前聚合块与每层一个累加器，不加载全部采样或全部块。
    """
    if block_size <= 0:
        raise ValueError("block_size 必须为正整数")
    if sample_count is None:
        sample_count = conn.execute(
            "SELECT COUNT(*) FROM samples WHERE test_id = ?", (test_id,)
        ).fetchone()[0]
    if not sample_count:
        return 0
    if first_rowid is None or last_rowid is None:
        row = conn.execute(
            "SELECT MIN(rowid) AS lo, MAX(rowid) AS hi FROM samples WHERE test_id = ?",
            (test_id,),
        ).fetchone()
        first_rowid, last_rowid = row["lo"], row["hi"]

    # 由采样数预先算出层数：level 1 每块 block_size 点，之后每层 block_size 个
    # 子块合并为一块，直到只剩一块（与原先 while len(prev) > 1 一致）。
    levels = 1
    block_count = (sample_count + block_size - 1) // block_size
    while block_count > 1 and block_size > 1:
        block_count = (block_count + block_size - 1) // block_size
        levels += 1

    # 生成器链：消费最上层时逐级拉取，所有层级的块边生成边写入。
    children = _iter_level1_blocks(
        conn, test_id, block_size, first_rowid, last_rowid
    )
    level = 1
    while level < levels:
        level += 1
        children = _iter_merged_blocks(conn, test_id, level, block_size, children)
    for _ in children:
        pass
    return levels


def range_summary(
    conn: sqlite3.Connection,
    test_id: int,
    q_start: int,
    q_end: int,
    max_level: int,
    block_size: int | None = None,
    metrics: Optional[dict] = None,
) -> Optional[Stats]:
    """计算半开区间 [q_start, q_end) 内采样的四点统计。

    递归策略：
    1. 取当前最粗层级与区间相交的块；
    2. 块完整落在区间内 -> 直接采用块摘要（命中缓存，不读采样）；
    3. 块被区间边界切开 -> 降到下一层递归；到 level 1 仍被切开
       时，按 rowid 范围读取该块内的原始采样。
    """
    block_size = block_size or config.BLOCK_SIZE
    if metrics is None:
        metrics = {"blocks_used": 0, "blocks_by_level": {}, "raw_rows_read": 0}

    if q_start >= q_end:
        return None

    level = max_level
    overall = Stats()

    while level >= 1:
        rows = conn.execute(
            """SELECT * FROM blocks
               WHERE test_id = ? AND level = ?
                 AND start_ts < ? AND end_ts >= ?
               ORDER BY block_index""",
            (test_id, level, q_end, q_start),
        ).fetchall()

        if not rows:
            # 本层没有相交块（例如查询区间只压在尾部半块之外），继续向下找
            level -= 1
            continue

        for row in rows:
            fully_inside = row["start_ts"] >= q_start and row["end_ts"] < q_end
            if fully_inside:
                overall = merge_stats(overall, Stats.from_row(row))
                metrics["blocks_used"] += 1
                metrics["blocks_by_level"][str(level)] = (
                    metrics["blocks_by_level"].get(str(level), 0) + 1
                )
                continue

            # 块被区间边界切开
            if level == 1:
                # 最细索引块也被切开 -> 唯一需要读原始采样的情形
                raw = conn.execute(
                    """SELECT ts, pressure FROM samples
                       WHERE test_id = ? AND rowid BETWEEN ? AND ?
                         AND ts >= ? AND ts < ?
                       ORDER BY rowid""",
                    (
                        test_id,
                        row["first_rowid"],
                        row["last_rowid"],
                        q_start,
                        q_end,
                    ),
                ).fetchall()
                metrics["raw_rows_read"] += len(raw)
                for r in raw:
                    overall = merge_stats(
                        overall, stats_from_sample(r["ts"], r["pressure"])
                    )
            else:
                # 递归处理该父块在更细一层中的子块，区间裁剪到父块覆盖范围。
                # 右端取 end_ts+1：当父块末点本就落在桶内时，保证最末子块
                # 仍能命中“完整落入”分支；右开排除由 level 1 的 ts < q_end 兜底，
                # 不会把桶外采样带进来。
                sub = range_summary(
                    conn,
                    test_id,
                    max(q_start, row["start_ts"]),
                    min(q_end, row["end_ts"] + 1),
                    level - 1,
                    block_size,
                    metrics,
                )
                if sub is not None:
                    overall = merge_stats(overall, sub)
        return overall if overall.sample_count else None

    return overall if overall.sample_count else None
