# Operations & Troubleshooting

Common checks used across modules.

## Kafka Checks

List topics:

```bash
./scripts/kafka/kafka_topics.sh --list
```

Describe a topic:

```bash
./scripts/kafka/kafka_topics.sh --describe --topic transactions_raw
```

## Spark Connector Checks

If Kafka/GCS integration fails, verify package compatibility and credentials.

```bash
spark-submit --version
```

- Ensure Kafka connector artifact matches Spark + Scala versions.
- Ensure `gcs-connector` is included for `gs://` paths.
- Ensure `GOOGLE_APPLICATION_CREDENTIALS` points to a valid key.

## Checkpoint Errors on GCS

For `CANNOT_LOAD_CHECKPOINT_FILE_MANAGER`:

1. Re-export `GOOGLE_APPLICATION_CREDENTIALS`.
2. Include `com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.28` in `--packages`.
3. Retry with a clean checkpoint path.

## dbt Runtime Errors

- Confirm Python is `3.11.x`.
- Reinstall dependencies from `dbt/requirements.txt`.
- Run `dbt debug` before `dbt run`.

## Airflow Runtime Errors

- Validate required env vars in `airflow/.env`.
- Confirm key file is mounted and readable inside containers.
- If `spark-submit` is not found, set `FRAUD_SPARK_SUBMIT_BIN` in `airflow/.env`.
