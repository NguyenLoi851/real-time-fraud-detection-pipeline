#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train and save a baseline fraud model for streaming inference")
    parser.add_argument("--input", default="data/transaction_log.csv", help="Input labeled CSV path")
    parser.add_argument("--model-output", default="ml/artifacts/fraud_rf_pipeline", help="Output path for saved Spark PipelineModel")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--train-ratio", type=float, default=0.8, help="Train split ratio")
    return parser.parse_args()


def prepare_features(df):
    clean_df = (
        df.withColumn("step", F.col("step").cast("int"))
        .withColumn("amount", F.coalesce(F.col("amount").cast("double"), F.lit(0.0)))
        .withColumn("oldbalanceOrg", F.coalesce(F.col("oldbalanceOrg").cast("double"), F.lit(0.0)))
        .withColumn("newbalanceOrig", F.coalesce(F.col("newbalanceOrig").cast("double"), F.lit(0.0)))
        .withColumn("oldbalanceDest", F.coalesce(F.col("oldbalanceDest").cast("double"), F.lit(0.0)))
        .withColumn("newbalanceDest", F.coalesce(F.col("newbalanceDest").cast("double"), F.lit(0.0)))
        .withColumn("type", F.coalesce(F.col("type"), F.lit("UNKNOWN")))
        .withColumn("isFraud", F.col("isFraud").cast("double"))
    )

    feature_df = (
        clean_df.withColumn("balance_change_ratio", F.when(F.col("oldbalanceOrg") > 0, F.col("amount") / F.col("oldbalanceOrg")).otherwise(F.lit(0.0)))
        .withColumn("is_new_merchant", F.when((F.col("oldbalanceDest") == 0.0) & (F.col("newbalanceDest") == 0.0), F.lit(1.0)).otherwise(F.lit(0.0)))
        .withColumn("origin_balance_delta", F.col("oldbalanceOrg") - F.col("newbalanceOrig"))
        .withColumn("dest_balance_delta", F.col("newbalanceDest") - F.col("oldbalanceDest"))
    )
    return feature_df


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder.appName("fraud-model-training")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_path}")

    raw_df = spark.read.option("header", True).csv(str(input_path))
    if "isFraud" not in raw_df.columns:
        raise ValueError("Input data must include 'isFraud' for supervised training")

    prepared_df = prepare_features(raw_df).na.fill({"isFraud": 0.0})

    train_df, test_df = prepared_df.randomSplit([args.train_ratio, 1 - args.train_ratio], seed=args.seed)

    type_indexer = StringIndexer(inputCol="type", outputCol="type_index", handleInvalid="keep")
    assembler = VectorAssembler(
        inputCols=[
            "step",
            "amount",
            "oldbalanceOrg",
            "newbalanceOrig",
            "oldbalanceDest",
            "newbalanceDest",
            "balance_change_ratio",
            "is_new_merchant",
            "origin_balance_delta",
            "dest_balance_delta",
            "type_index",
        ],
        outputCol="features",
        handleInvalid="keep",
    )
    classifier = RandomForestClassifier(
        labelCol="isFraud",
        featuresCol="features",
        predictionCol="prediction",
        probabilityCol="probability",
        numTrees=120,
        maxDepth=8,
        seed=args.seed,
    )

    pipeline = Pipeline(stages=[type_indexer, assembler, classifier])
    model = pipeline.fit(train_df)

    predictions = model.transform(test_df)
    evaluator = BinaryClassificationEvaluator(labelCol="isFraud", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
    auc = evaluator.evaluate(predictions)

    output_path = Path(args.model_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    model.write().overwrite().save(str(output_path))

    print(f"Model saved to: {output_path}")
    print(f"Validation AUC: {auc:.4f}")

    spark.stop()


if __name__ == "__main__":
    main()
