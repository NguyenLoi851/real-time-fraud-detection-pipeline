#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID to your GCP project id}"
REGION="${GCP_REGION:-us-central1}"
BATCH_ID="${GCP_DATAPROC_STREAMING_BATCH_ID:-fraud-streaming-batch-$(date +%Y%m%d%H%M%S)}"
SHUFFLE_PARTITIONS="${FRAUD_SHUFFLE_PARTITIONS:-8}"
OUTPUT_PARTITIONS="${FRAUD_OUTPUT_PARTITIONS:-1}"
GCS_BUCKET="${GCP_GCS_BUCKET:?Set GCP_GCS_BUCKET to the bucket that stores model artifacts and lake outputs}"
DEPS_BUCKET="${GCP_DATAPROC_DEPS_BUCKET:-$GCS_BUCKET}"
INPUT_SUBSCRIPTION="${FRAUD_INPUT_SUBSCRIPTION:?Set FRAUD_INPUT_SUBSCRIPTION to a Pub/Sub subscription id or full path}"
ALERTS_TOPIC="${FRAUD_ALERTS_TOPIC:-fraud_alerts}"
MODEL_PATH="${FRAUD_MODEL_PATH:-gs://${GCS_BUCKET}/ml/artifacts/fraud_rf_pipeline}"
RAW_PATH="${FRAUD_RAW_PATH:-gs://${GCS_BUCKET}/lake/bronze/transactions_raw}"
SCORED_PATH="${FRAUD_SCORED_PATH:-gs://${GCS_BUCKET}/lake/silver/scored_transactions}"
ALERTS_PATH="${FRAUD_ALERTS_PATH:-gs://${GCS_BUCKET}/lake/gold/fraud_alerts}"
EXTRA_SPARK_PROPERTIES="${GCP_DATAPROC_SPARK_PROPERTIES:-}"
ALLOW_PARALLEL_STREAMING="${FRAUD_ALLOW_PARALLEL_STREAMING:-false}"

# Avoid known object-store write race conditions on append outputs.
DEFAULT_SPARK_PROPERTIES="spark.speculation=false,spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs=false"
SPARK_PROPERTIES="$DEFAULT_SPARK_PROPERTIES"
if [[ -n "$EXTRA_SPARK_PROPERTIES" ]]; then
  SPARK_PROPERTIES="${DEFAULT_SPARK_PROPERTIES},${EXTRA_SPARK_PROPERTIES}"
fi

if [[ "$MODEL_PATH" == gs://* ]]; then
  if ! gsutil ls "${MODEL_PATH%/}/metadata" >/dev/null 2>&1; then
    echo "ERROR: Model artifact not found at $MODEL_PATH"
    echo "Expected Spark PipelineModel metadata under: ${MODEL_PATH%/}/metadata"
    echo "Upload a trained model first, for example:"
    echo "  gsutil -m cp -r ml/artifacts/fraud_rf_pipeline \"$MODEL_PATH\""
    exit 1
  fi

  model_meta="$(gsutil cat "${MODEL_PATH%/}/metadata/part-*.txt" 2>/dev/null || true)"
  model_spark_version="$(printf '%s' "$model_meta" | sed -n 's/.*"sparkVersion":"\([^"]*\)".*/\1/p' | head -n1)"
  if [[ -n "$model_spark_version" ]]; then
    model_major="${model_spark_version%%.*}"
    if [[ "$model_major" -ge 4 ]]; then
      echo "ERROR: Model at $MODEL_PATH was trained with Spark $model_spark_version, which is incompatible with current Dataproc runtime."
      echo "Rebuild the model using Dataproc Spark 3.x, then rerun streaming."
      echo "Recommended: bash scripts/gcp/dataproc/submit_model_train_bootstrap.sh"
      exit 1
    fi
  fi
fi

script_args=(
  "--pubsub-project-id" "$PROJECT_ID"
  "--input-subscription" "$INPUT_SUBSCRIPTION"
  "--alerts-topic" "$ALERTS_TOPIC"
  "--model-path" "$MODEL_PATH"
  "--datalake-raw-path" "$RAW_PATH"
  "--datalake-scored-path" "$SCORED_PATH"
  "--datalake-alerts-path" "$ALERTS_PATH"
  "--shuffle-partitions" "$SHUFFLE_PARTITIONS"
  "--output-partitions" "$OUTPUT_PARTITIONS"
)

batch_flags=(
  --project="$PROJECT_ID"
  --region="$REGION"
  --deps-bucket="$DEPS_BUCKET"
)

if [[ -n "$SPARK_PROPERTIES" ]]; then
  batch_flags+=("--properties=${SPARK_PROPERTIES}")
fi

# Dataproc batch ids must be unique per region; auto-suffix on collision for safe reruns.
if gcloud dataproc batches describe "$BATCH_ID" --project="$PROJECT_ID" --region="$REGION" >/dev/null 2>&1; then
  BATCH_ID="${BATCH_ID}-retry-$(date +%Y%m%d%H%M%S)"
fi

# Guard against accidental duplicate streaming jobs writing to identical GCS prefixes.
if [[ "$ALLOW_PARALLEL_STREAMING" != "true" ]]; then
  existing_batches="$(gcloud dataproc batches list \
    --project="$PROJECT_ID" \
    --region="$REGION" \
    --filter='state=RUNNING AND labels.pipeline=fraud AND labels.job=streaming' \
    --format='value(name)' 2>/dev/null || true)"
  if [[ -n "${existing_batches//[[:space:]]/}" ]]; then
    echo "ERROR: Detected running streaming Dataproc batch(es) with labels pipeline=fraud,job=streaming."
    echo "Running a second streaming writer against the same GCS output paths can trigger GCS 412 Precondition Failed."
    echo "Stop existing batch(es) first, or set FRAUD_ALLOW_PARALLEL_STREAMING=true if you intentionally isolated outputs."
    echo "$existing_batches"
    exit 1
  fi
fi

batch_flags+=("--batch=${BATCH_ID}")
batch_flags+=("--labels=pipeline=fraud,job=streaming")

if [[ -n "${GCP_DATAPROC_SERVICE_ACCOUNT:-}" ]]; then
  batch_flags+=("--service-account=${GCP_DATAPROC_SERVICE_ACCOUNT}")
fi

if [[ -n "${GCP_DATAPROC_SUBNET:-}" ]]; then
  batch_flags+=("--subnet=${GCP_DATAPROC_SUBNET}")
fi

gcloud dataproc batches submit pyspark "$REPO_ROOT/streaming/pyspark_fraud_streaming.py" "${batch_flags[@]}" -- "${script_args[@]}"
