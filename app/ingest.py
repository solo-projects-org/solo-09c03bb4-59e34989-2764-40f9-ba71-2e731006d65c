"""记录仪 CSV 导入与校验。

要求：
- 首行表头，需能识别出时间戳列与压力列（支持中英文表头，见 config）
- 毫秒时间戳为整数且严格递增（重复或倒序即拒绝整次导入）
- 压力为有限浮点数，单位 kPa

实现约束：导入大文件时全程流式处理——
- CSV 从二进制文件对象按行解码读取，不在内存中保留整份文件；
- 采样按固定大小分批 INSERT，不保留全部采样；
- 摘要构建按块流式扫描采样并逐层合并（见 summaries.build_summaries）。
整个导入（试验元信息、采样、各级摘要）处于同一事务，任何一行校验失败都会
回滚，不会留下半成品。
"""

from __future__ import annotations

import csv
import io
import math
import sqlite3
from typing import BinaryIO, Iterator

from . import config
from .summaries import build_summaries

# 每批写入 samples 的行数：限制瞬时内存，同时减少 SQL 往返
INSERT_BATCH_SIZE = 4096


class CsvValidationError(ValueError):
    """CSV 内容不满足导入契约。"""


def _resolve_columns(header: list[str]) -> tuple[int, int]:
    ts_idx = p_idx = -1
    for i, name in enumerate(header):
        key = name.strip().lower()
        if ts_idx == -1 and key in config.TIMESTAMP_COLUMNS:
            ts_idx = i
        if p_idx == -1 and key in config.PRESSURE_COLUMNS:
            p_idx = i
    missing = []
    if ts_idx == -1:
        missing.append("时间戳列")
    if p_idx == -1:
        missing.append("压力列")
    if missing:
        raise CsvValidationError(
            "表头缺少 " + "、".join(missing) + f"；实际表头：{header}"
        )
    return ts_idx, p_idx


def _parse_int_ms(raw: str, line_no: int) -> int:
    raw = raw.strip()
    if not raw:
        raise CsvValidationError(f"第 {line_no} 行时间戳为空")
    try:
        # 拒绝 1.5、1.0 这类非整数毫秒时间戳
        value = int(raw)
    except ValueError:
        raise CsvValidationError(
            f"第 {line_no} 行时间戳不是整数毫秒：{raw!r}"
        ) from None
    return value


def _parse_pressure(raw: str, line_no: int) -> float:
    raw = raw.strip()
    if not raw:
        raise CsvValidationError(f"第 {line_no} 行压力为空")
    try:
        value = float(raw)
    except ValueError:
        raise CsvValidationError(f"第 {line_no} 行压力不是数值：{raw!r}") from None
    if not math.isfinite(value):
        raise CsvValidationError(f"第 {line_no} 行压力不是有限值：{raw!r}")
    return value


def _iter_sample_rows(source: BinaryIO, encoding: str) -> Iterator[tuple[int, float]]:
    """流式解析 CSV，逐行返回 (ts_ms, pressure_kpa)。

    TextIOWrapper 会按需解码，csv.reader 也只保留当前行，因此内存占用与
    文件大小无关。source 必须是可 seek 的二进制文件对象，以便编码回退时
    从头重读。
    """
    source.seek(0)
    text = io.TextIOWrapper(source, encoding=encoding, newline="")
    try:
        reader = csv.reader(text)
        try:
            header = next(reader)
        except StopIteration:
            raise CsvValidationError("CSV 为空，缺少表头") from None
        if not header or not any(cell.strip() for cell in header):
            raise CsvValidationError("CSV 表头为空")

        ts_idx, p_idx = _resolve_columns(header)
        width = len(header)

        prev_ts: int | None = None
        line_no = 1
        for row in reader:
            line_no += 1
            if not row or not any(cell.strip() for cell in row):
                continue  # 容忍空行
            if len(row) < width:
                raise CsvValidationError(
                    f"第 {line_no} 行列数 {len(row)} 少于表头列数 {width}"
                )
            ts = _parse_int_ms(row[ts_idx], line_no)
            pressure = _parse_pressure(row[p_idx], line_no)
            if prev_ts is not None and ts <= prev_ts:
                raise CsvValidationError(
                    f"第 {line_no} 行时间戳 {ts} 未严格递增（上一行为 {prev_ts}）"
                )
            prev_ts = ts
            yield ts, pressure
    finally:
        # 只解除包装，不关闭调用方传入的文件对象
        try:
            text.detach()
        except (ValueError, OSError):
            pass


def parse_csv(content: bytes) -> list[tuple[int, float]]:
    """解析并严格校验 CSV，返回 (ts_ms, pressure_kpa) 序列。

    仅供小样本 / 单元测试使用；HTTP 导入走 import_test 的流式路径，
    不会调用本函数。
    """
    source = io.BytesIO(content)
    try:
        return list(_iter_sample_rows(source, "utf-8-sig"))
    except UnicodeDecodeError:
        # 部分记录仪在 Windows 下导出 GBK；UTF-8 中途失败时从头重读
        source.seek(0)
        return list(_iter_sample_rows(source, "gbk"))


def _flush_samples(
    conn: sqlite3.Connection,
    batch: list[tuple[int, int, float]],
    first_rid: int | None,
    test_id: int,
) -> tuple[int, int | None]:
    """写入一批采样，返回 (本批首 rowid, 目前最后 rowid)。"""
    conn.executemany(
        "INSERT INTO samples (test_id, ts, pressure) VALUES (?, ?, ?)", batch
    )
    last_rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    if first_rid is None:
        # 首个批次：直接查本试验的最小 rowid，不依赖 rowid 连续这一假设
        first_rid = conn.execute(
            "SELECT MIN(rowid) FROM samples WHERE test_id = ?", (test_id,)
        ).fetchone()[0]
    batch.clear()
    return first_rid, last_rid


def _import_stream(
    conn: sqlite3.Connection,
    name: str,
    filename: str,
    source: BinaryIO,
    block_size: int,
    encoding: str,
) -> dict:
    """执行一次流式导入，返回写入结果；异常时由调用方回滚 savepoint。"""
    cur = conn.execute(
        "INSERT INTO tests (name, source_filename, sample_count, start_ts, end_ts, max_level) "
        "VALUES (?, ?, 0, 0, 0, 0)",
        (name, filename),
    )
    test_id = cur.lastrowid

    batch: list[tuple[int, int, float]] = []
    sample_count = 0
    first_ts: int | None = None
    last_ts: int | None = None
    first_rid: int | None = None
    last_rid: int | None = None

    rows = _iter_sample_rows(source, encoding)
    try:
        for ts, pressure in rows:
            if first_ts is None:
                first_ts = ts
            last_ts = ts
            sample_count += 1
            batch.append((test_id, ts, pressure))
            if len(batch) >= INSERT_BATCH_SIZE:
                first_rid, last_rid = _flush_samples(
                    conn, batch, first_rid, test_id
                )
    finally:
        rows.close()

    if batch:
        first_rid, last_rid = _flush_samples(conn, batch, first_rid, test_id)
    if sample_count == 0 or first_ts is None or last_ts is None:
        raise CsvValidationError("CSV 没有任何有效采样行")

    conn.execute(
        "UPDATE tests SET sample_count = ?, start_ts = ?, end_ts = ? WHERE id = ?",
        (sample_count, first_ts, last_ts, test_id),
    )
    max_level = build_summaries(
        conn,
        test_id,
        block_size,
        first_rowid=first_rid,
        last_rowid=last_rid,
        sample_count=sample_count,
    )
    conn.execute(
        "UPDATE tests SET max_level = ? WHERE id = ?", (max_level, test_id)
    )
    return {
        "test_id": test_id,
        "sample_count": sample_count,
        "start_ts": first_ts,
        "end_ts": last_ts,
        "max_level": max_level,
    }


def import_test(
    conn: sqlite3.Connection,
    name: str,
    filename: str,
    content: bytes | BinaryIO,
    block_size: int | None = None,
) -> dict:
    """完整导入一次试验：采样入库 + 多级摘要，单事务完成。

    content 可以是字节串（便利接口，主要供测试/小文件）或可 seek 的
    二进制文件对象（HTTP 上传路径使用，全程不整份读入内存）。

    整个导入放在 savepoint 内；UTF-8 解码中途失败时回滚到 savepoint 再用
    GBK 重读，成功后才释放。任何校验/唯一约束错误都会撤销本次导入的
    试验信息、采样和摘要，不留部分数据。若调用方没有现成事务，本函数会
    自行开启并在成功后提交，保证返回响应时数据已可见。
    """
    block_size = block_size or config.BLOCK_SIZE
    source: BinaryIO
    if isinstance(content, (bytes, bytearray, memoryview)):
        source = io.BytesIO(bytes(content))
    else:
        source = content

    # 若调用方已有事务（例如被更大事务包裹），只使用嵌套 savepoint，由调用方
    # 决定提交/回滚；否则自行开启外层事务并在成功后提交，避免 HTTP 响应先于
    # session 依赖清理返回、客户端紧接着查询却看不到刚导入的数据。
    owns_transaction = not conn.in_transaction
    if owns_transaction:
        conn.execute("BEGIN IMMEDIATE")
    conn.execute("SAVEPOINT import_test")
    try:
        try:
            result = _import_stream(
                conn, name, filename, source, block_size, "utf-8-sig"
            )
        except UnicodeDecodeError:
            # UTF-8 可能在读到最后一行附近才发现非法字节，先撤销已写入的行
            conn.execute("ROLLBACK TO SAVEPOINT import_test")
            try:
                result = _import_stream(
                    conn, name, filename, source, block_size, "gbk"
                )
            except UnicodeDecodeError as exc:
                raise CsvValidationError(
                    "CSV 编码无法识别（需 UTF-8 或 GBK）"
                ) from exc
    except sqlite3.IntegrityError as exc:
        conn.execute("ROLLBACK TO SAVEPOINT import_test")
        conn.execute("RELEASE SAVEPOINT import_test")
        if owns_transaction:
            conn.rollback()
        raise CsvValidationError(f"试验名称 {name!r} 已存在") from exc
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT import_test")
        conn.execute("RELEASE SAVEPOINT import_test")
        if owns_transaction:
            conn.rollback()
        raise
    else:
        conn.execute("RELEASE SAVEPOINT import_test")

    test_id = result["test_id"]
    block_counts = dict(
        conn.execute(
            "SELECT level, COUNT(*) FROM blocks WHERE test_id = ? GROUP BY level",
            (test_id,),
        ).fetchall()
    )
    imported_at = conn.execute(
        "SELECT imported_at FROM tests WHERE id = ?", (test_id,)
    ).fetchone()[0]
    response = {
        "id": test_id,
        "name": name,
        "source_filename": filename,
        "sample_count": result["sample_count"],
        "start_ts": result["start_ts"],
        "end_ts": result["end_ts"],
        "max_level": result["max_level"],
        "imported_at": imported_at,
        "block_counts": {str(k): v for k, v in sorted(block_counts.items())},
    }
    if owns_transaction:
        conn.commit()
    return response
