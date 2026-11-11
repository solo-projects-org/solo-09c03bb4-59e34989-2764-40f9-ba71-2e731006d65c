#!/usr/bin/env python3
"""生成启泵试验样例 CSV（合成数据，模拟记录仪导出格式）。

运行（默认生成 2 万点演示 + 60 点小样本）：
    python3 scripts/generate_sample.py

生成用于验证大文件流式导入的 200 万行样例（约 44 MB）：
    python3 scripts/generate_sample.py --rows 2000000 --out sample_data/large_demo.csv

输出列：timestamp_ms,pressure_kpa；毫秒时间戳严格递增。
"""

from __future__ import annotations

import argparse
import csv
import math
from pathlib import Path

BASE_TS = 1_731_300_000_000  # 固定基准，保证样例可复现
DEFAULT_DIR = Path(__file__).resolve().parent.parent / "sample_data"


def pressure_at(t_ms: int) -> float:
    """模拟启泵过程：静压 -> 启泵爬升 -> 水锤衰减振荡，含一次低压深谷。"""
    t = t_ms
    if t < 2000:
        # 启泵前管网静压，微小波动
        p = 100.0 + 1.5 * math.sin(2 * math.pi * 0.5 * t / 1000.0)
    elif t < 6000:
        # 启泵后近似线性爬升
        p = 100.0 + 220.0 * (t - 2000) / 4000.0
    else:
        dt = t - 6000
        steady = 320.0
        oscillation = 60.0 * math.exp(-dt / 2500.0) * math.sin(
            2 * math.pi * 2.0 * dt / 1000.0
        )
        # 启泵瞬间约 50ms 处的一次低压水锤深谷
        dip = -45.0 * math.exp(-((dt - 50) / 25.0) ** 2)
        p = steady + oscillation + dip
    return round(p, 3)


def write_csv(path: Path, count: int, interval_ms: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_ms", "pressure_kpa"])
        for i in range(count):
            ts = BASE_TS + i * interval_ms
            writer.writerow([ts, pressure_at(i * interval_ms)])
    print(f"wrote {path} ({count} rows, {path.stat().st_size / 1024 / 1024:.1f} MiB)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--rows",
        type=int,
        default=None,
        help="生成指定行数的单个文件（配合 --out）；不指定则生成默认演示样例",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=None,
        help="自定义输出文件路径；指定后只生成该文件",
    )
    parser.add_argument(
        "--interval-ms", type=int, default=10, help="采样间隔毫秒（默认 10）"
    )
    args = parser.parse_args()

    if args.out is not None or args.rows is not None:
        out = args.out or (DEFAULT_DIR / "custom_demo.csv")
        write_csv(out, count=args.rows or 20_000, interval_ms=args.interval_ms)
        return

    write_csv(DEFAULT_DIR / "pump_start_demo.csv", count=20_000, interval_ms=10)
    write_csv(DEFAULT_DIR / "small_demo.csv", count=60, interval_ms=100)


if __name__ == "__main__":
    main()
