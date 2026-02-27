#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read and preview processed data from local data lake")
    parser.add_argument(
        "--path",
        default="data/lake/silver/scored_transactions",
        help="Input data lake path (default: silver scored transactions)",
    )
    parser.add_argument(
        "--format",
        default="parquet",
        choices=["parquet", "json", "csv"],
        help="Input data format",
    )
    parser.add_argument("--limit", type=int, default=20, help="Number of rows to display")
    parser.add_argument("--only-alerts", action="store_true", help="Show only rows where is_alert = true")
    parser.add_argument("--show-schema", action="store_true", help="Print schema before preview")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    source_path = Path(args.path)
    if not source_path.exists():
        raise FileNotFoundError(f"Data lake path not found: {source_path}")

    spark = SparkSession.builder.appName("read-datalake-sample").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format(args.format).load(str(source_path))

    if args.show_schema:
        df.printSchema()

    if args.only_alerts:
        if "is_alert" not in df.columns:
            raise ValueError("Column 'is_alert' not found. Use a dataset that contains alert flags.")
        df = df.filter(F.col("is_alert") == F.lit(True))

    order_col = "event_ts" if "event_ts" in df.columns else None
    preview_df = df.orderBy(F.col(order_col).desc()) if order_col else df

    print(f"Preview from: {source_path}")
    preview_df.show(args.limit, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
