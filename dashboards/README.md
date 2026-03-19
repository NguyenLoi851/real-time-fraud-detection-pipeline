# BI and Reporting

## Purpose

Build and operate a Tableau dashboard from BigQuery warehouse outputs.

## Prerequisites

- BigQuery dataset available (default `fraud_analytics`)
- dbt marts built (at minimum `mart_fraud_hourly_kpis`)
- BigQuery read access

For execution prerequisites and pipeline order, see [../docs/runbook-gcp.md](../docs/runbook-gcp.md).

## Source Tables

- Primary: `fraud_analytics.mart_fraud_hourly_kpis`
- Optional drill-down: `fraud_analytics.fct_fraud_alerts`

## Recommended Charts

Use `event_hour_utc` as default time dimension.

1. Hourly transaction volume (`SUM(transaction_count)`)
2. Hourly alert volume (`SUM(alert_count)`)
3. Alert rate trend (`AVG(alert_rate_recomputed)`)
4. Fraud score trend (`AVG(avg_fraud_score)`, `MAX(max_fraud_score)`)
5. P95 score trend (`AVG(p95_fraud_score)`)
6. Transaction type breakdown (`transaction_type`)

## Recommended Filters

- Date range on `event_hour_utc`
- Drop-down filter on `transaction_type`

## Build Steps

1. Open Tableau Desktop, Tableau Cloud, or Tableau Public.
2. Connect to BigQuery and select `fraud_analytics.mart_fraud_hourly_kpis`.
3. Add charts and filters listed above.
4. Save workbook/dashboard (example: `Fraud Monitoring - Hourly KPI Dashboard`).

## Freshness

- Automatic: when Airflow DAG is enabled.
- Manual refresh path: batch job -> BigQuery load -> dbt marts.
