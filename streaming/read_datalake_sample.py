#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read and preview processed data from local or GCS data lake")
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

    is_gcs_path = args.path.startswith("gs://")
    source_path = args.path if is_gcs_path else str(Path(args.path))

    if is_gcs_path and not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip():
        raise ValueError(
            "GCS path detected but GOOGLE_APPLICATION_CREDENTIALS is not set. "
            "Set it to your service account key JSON file path."
        )

    if not is_gcs_path:
        local_path = Path(source_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Data lake path not found: {local_path}")

    builder = SparkSession.builder.appName("read-datalake-sample").master("local[*]")
    if is_gcs_path:
        builder = (
            builder.config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
            .config(
                "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
                os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", ""),
            )
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format(args.format).load(source_path)

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
