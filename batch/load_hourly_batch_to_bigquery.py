#!/usr/bin/env python3
"""
Goal: Load hourly batch Parquet outputs from GCS into BigQuery tables.
Reads curated datasets produced by hourly batch processing and inserts/appends
them into corresponding BigQuery tables in the fraud analytics dataset.
"""
from __future__ import annotations

import argparse
from typing import Iterable

try:
    from google.api_core.exceptions import NotFound as ApiNotFound
    from google.cloud import bigquery
    from google.cloud.exceptions import NotFound
except ImportError:  # Optional in Dataproc Spark runtime.
    ApiNotFound = Exception
    bigquery = None
    NotFound = Exception

try:
    from pyspark.sql import SparkSession
except ImportError:  # Optional for local BigQuery client mode.
    SparkSession = None


DEFAULT_TABLES = [
    "curated_scored",
    "retraining_dataset",
    "monitoring_hourly",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load hourly batch Parquet outputs from GCS into BigQuery tables"
    )
    parser.add_argument(
        "--project-id",
        required=True,
        help="BigQuery project id",
    )
    parser.add_argument(
        "--runtime-mode",
        default="local",
        choices=["local", "gcp-native"],
        help="Execution mode: local BigQuery client or Dataproc Spark connector",
    )
    parser.add_argument(
        "--dataset",
        default="fraud_analytics",
        help="BigQuery dataset name for raw batch tables",
    )
    parser.add_argument(
        "--gcs-output-base",
        required=True,
        help="Base GCS path of hourly batch outputs, for example gs://<gold_bucket>/hourly_batch",
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=DEFAULT_TABLES,
        help="Table names to load from subfolders under gcs-output-base",
    )
    parser.add_argument(
        "--write-disposition",
        default="WRITE_TRUNCATE",
        choices=["WRITE_TRUNCATE", "WRITE_APPEND", "WRITE_EMPTY"],
        help="BigQuery write mode for each target table",
    )
    parser.add_argument(
        "--create-dataset-if-missing",
        action="store_true",
        help="Create BigQuery dataset if it does not exist",
    )
    parser.add_argument(
        "--temporary-gcs-bucket",
        default="",
        help="Optional temporary GCS bucket for Spark BigQuery connector writes",
    )
    return parser.parse_args()


def normalize_gcs_uri(uri: str) -> str:
    value = uri.strip()
    if not value.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}. Expected format gs://<bucket>/<path>")
    return value.rstrip("/")


def ensure_dataset_local(client: "bigquery.Client", project_id: str, dataset: str, create_if_missing: bool) -> None:
    dataset_ref = f"{project_id}.{dataset}"
    try:
        client.get_dataset(dataset_ref)
        print(f"[bq-load] Dataset exists: {dataset_ref}", flush=True)
    except NotFound:
        if not create_if_missing:
            raise ValueError(
                f"Dataset not found: {dataset_ref}. Use --create-dataset-if-missing to auto-create."
            )
        created = client.create_dataset(bigquery.Dataset(dataset_ref), exists_ok=True)
        print(f"[bq-load] Created dataset: {created.full_dataset_id}", flush=True)


def build_source_uris(base_uri: str, table_name: str) -> list[str]:
    table_base = f"{base_uri}/{table_name}"
    return [
        f"{table_base}/batch_hour_utc=*/*.parquet",
        f"{table_base}/*/*.parquet",
        f"{table_base}/*.parquet",
    ]


def load_table(
    client: "bigquery.Client",
    project_id: str,
    dataset: str,
    table_name: str,
    source_uris: list[str],
    write_disposition: str,
) -> None:
    table_id = f"{project_id}.{dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )

    last_not_found: Exception | None = None
    for source_uri in source_uris:
        print(f"[bq-load] Loading {source_uri} -> {table_id}", flush=True)
        try:
            job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
            result = job.result()

            destination_table = client.get_table(table_id)
            print(
                f"[bq-load] Completed {table_id}: rows={destination_table.num_rows}, "
                f"state={result.state}",
                flush=True,
            )
            return
        except ApiNotFound as exc:
            last_not_found = exc
            print(f"[bq-load] No files matched: {source_uri}", flush=True)

    searched = ", ".join(source_uris)
    raise ValueError(
        f"No parquet files found for table '{table_name}'. "
        f"Checked URIs: {searched}"
    ) from last_not_found


def load_table_with_spark(
    spark: "SparkSession",
    project_id: str,
    dataset: str,
    table_name: str,
    source_uris: list[str],
    write_disposition: str,
    temporary_gcs_bucket: str,
) -> None:
    table_id = f"{project_id}.{dataset}.{table_name}"
    write_mode_map = {
        "WRITE_TRUNCATE": "overwrite",
        "WRITE_APPEND": "append",
        "WRITE_EMPTY": "errorifexists",
    }
    spark_mode = write_mode_map[write_disposition]

    last_error: Exception | None = None
    df = None
    for source_uri in source_uris:
        print(f"[bq-load] Reading parquet from {source_uri}", flush=True)
        try:
            df = spark.read.parquet(source_uri)
            break
        except Exception as exc:
            last_error = exc
            print(f"[bq-load] No readable parquet at {source_uri}", flush=True)

    if df is None:
        searched = ", ".join(source_uris)
        raise ValueError(
            f"No parquet files found for table '{table_name}'. Checked URIs: {searched}"
        ) from last_error

    writer = (
        df.write.format("bigquery")
        .mode(spark_mode)
        .option("table", table_id)
    )

    if temporary_gcs_bucket:
        writer = writer.option("temporaryGcsBucket", temporary_gcs_bucket)

    print(
        f"[bq-load] Writing Spark DataFrame -> {table_id} (mode={spark_mode})",
        flush=True,
    )
    writer.save()
    print(f"[bq-load] Completed Spark load for {table_id}", flush=True)


def validate_tables(tables: Iterable[str]) -> list[str]:
    unique_tables: list[str] = []
    for table in tables:
        normalized = table.strip()
        if not normalized:
            continue
        if normalized not in unique_tables:
            unique_tables.append(normalized)
    if not unique_tables:
        raise ValueError("No valid tables provided.")
    return unique_tables


def main() -> None:
    args = parse_args()

    gcs_output_base = normalize_gcs_uri(args.gcs_output_base)
    tables = validate_tables(args.tables)

    print("[bq-load] Starting load process...", flush=True)
    print(f"[bq-load] Project: {args.project_id}", flush=True)
    print(f"[bq-load] Runtime mode: {args.runtime_mode}", flush=True)
    print(f"[bq-load] Dataset: {args.dataset}", flush=True)
    print(f"[bq-load] GCS base: {gcs_output_base}", flush=True)
    print(f"[bq-load] Tables: {', '.join(tables)}", flush=True)
    print(f"[bq-load] Write disposition: {args.write_disposition}", flush=True)

    if args.runtime_mode == "local":
        if bigquery is None:
            raise RuntimeError(
                "google-cloud-bigquery is not installed. "
                "Install dependencies or run with --runtime-mode gcp-native on Dataproc."
            )

        client = bigquery.Client(project=args.project_id)
        ensure_dataset_local(client, args.project_id, args.dataset, args.create_dataset_if_missing)

        for table_name in tables:
            source_uris = build_source_uris(gcs_output_base, table_name)
            load_table(
                client=client,
                project_id=args.project_id,
                dataset=args.dataset,
                table_name=table_name,
                source_uris=source_uris,
                write_disposition=args.write_disposition,
            )
    else:
        if SparkSession is None:
            raise RuntimeError(
                "PySpark is not available for gcp-native mode. "
                "Use Dataproc Serverless with pyspark submit."
            )

        spark = SparkSession.builder.appName("hourly-bq-loader").getOrCreate()
        try:
            for table_name in tables:
                source_uris = build_source_uris(gcs_output_base, table_name)
                load_table_with_spark(
                    spark=spark,
                    project_id=args.project_id,
                    dataset=args.dataset,
                    table_name=table_name,
                    source_uris=source_uris,
                    write_disposition=args.write_disposition,
                    temporary_gcs_bucket=args.temporary_gcs_bucket,
                )
        finally:
            spark.stop()

    print("[bq-load] BigQuery load process completed.", flush=True)


if __name__ == "__main__":
    main()
