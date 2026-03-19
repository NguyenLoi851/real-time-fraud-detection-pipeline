#!/usr/bin/env python3
"""
Goal: Load hourly batch Parquet outputs from GCS into BigQuery tables.
Reads curated datasets produced by hourly batch processing and inserts/appends
them into corresponding BigQuery tables in the fraud analytics dataset.
"""
from __future__ import annotations

import argparse
from typing import Iterable

from google.cloud import bigquery
from google.cloud.exceptions import NotFound


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
    return parser.parse_args()


def normalize_gcs_uri(uri: str) -> str:
    value = uri.strip()
    if not value.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}. Expected format gs://<bucket>/<path>")
    return value.rstrip("/")


def ensure_dataset(client: bigquery.Client, project_id: str, dataset: str, create_if_missing: bool) -> None:
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


def build_source_uri(base_uri: str, table_name: str) -> str:
    return f"{base_uri}/{table_name}/*.parquet"


def load_table(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    table_name: str,
    source_uri: str,
    write_disposition: str,
) -> None:
    table_id = f"{project_id}.{dataset}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=write_disposition,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        autodetect=True,
    )

    print(f"[bq-load] Loading {source_uri} -> {table_id}", flush=True)
    job = client.load_table_from_uri(source_uri, table_id, job_config=job_config)
    result = job.result()

    destination_table = client.get_table(table_id)
    print(
        f"[bq-load] Completed {table_id}: rows={destination_table.num_rows}, "
        f"state={result.state}",
        flush=True,
    )


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
    print(f"[bq-load] Dataset: {args.dataset}", flush=True)
    print(f"[bq-load] GCS base: {gcs_output_base}", flush=True)
    print(f"[bq-load] Tables: {', '.join(tables)}", flush=True)
    print(f"[bq-load] Write disposition: {args.write_disposition}", flush=True)

    client = bigquery.Client(project=args.project_id)
    ensure_dataset(client, args.project_id, args.dataset, args.create_dataset_if_missing)

    for table_name in tables:
        source_uri = build_source_uri(gcs_output_base, table_name)
        load_table(
            client=client,
            project_id=args.project_id,
            dataset=args.dataset,
            table_name=table_name,
            source_uri=source_uri,
            write_disposition=args.write_disposition,
        )

    print("[bq-load] BigQuery load process completed.", flush=True)


if __name__ == "__main__":
    main()
