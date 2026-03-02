# BI and Reporting (Roadmap Step 9)

This guide helps you complete roadmap **Step 9** first, without waiting for Step 8 (Airflow).

You will build a Looker Studio dashboard directly on BigQuery tables/views produced by Step 6 + Step 7.

## 1) Prerequisites

- BigQuery dataset exists (default: `fraud_analytics`).
- dbt models are built (at minimum `mart_fraud_hourly_kpis`).
- Your account has BigQuery read access.

If needed, rebuild dbt models:

```bash
cd dbt
dbt run --select marts
dbt test --select mart_fraud_hourly_kpis
```

## 2) BI Source Tables

Primary source for dashboard cards:

- `fraud_analytics.mart_fraud_hourly_kpis`

Useful supporting source (optional drill-down):

- `fraud_analytics.fct_fraud_alerts`

## 3) Recommended Looker Studio Charts

Use `event_hour_utc` as the default time dimension.

1. **Hourly Transaction Volume**
   - Dimension: `event_hour_utc`
   - Metric: `SUM(transaction_count)`

2. **Hourly Alert Volume**
   - Dimension: `event_hour_utc`
   - Metric: `SUM(alert_count)`

3. **Alert Rate Trend**
   - Dimension: `event_hour_utc`
   - Metric: `AVG(alert_rate_recomputed)`

4. **Fraud Score Trend**
   - Dimension: `event_hour_utc`
   - Metrics: `AVG(avg_fraud_score)`, `MAX(max_fraud_score)`

5. **P95 Score Trend**
   - Dimension: `event_hour_utc`
   - Metric: `AVG(p95_fraud_score)`

6. **Transaction Type Breakdown**
   - Dimension: `transaction_type`
   - Metrics: `SUM(transaction_count)`, `SUM(alert_count)`

## 4) Suggested Dashboard Filters

- Date range filter on `event_hour_utc`
- Drop-down filter on `transaction_type`

## 5) How to Build the Report in Looker Studio

1. Open Looker Studio: https://lookerstudio.google.com/
2. Click **Create** → **Report**.
3. Add data source:
   - Connector: **BigQuery**
   - Project: your GCP project
   - Dataset: `fraud_analytics`
   - Table: `mart_fraud_hourly_kpis`
4. In the data source schema, verify:
   - `event_hour_utc` is **Date & Time**
   - rate/score fields are **Number**
5. Add charts listed in Section 3 and assign dimensions/metrics exactly.
6. Add controls:
   - **Date range control** (top of report)
   - **Drop-down list control** for `transaction_type`
7. Save report with a clear name, for example:
   - `Fraud Monitoring - Hourly KPI Dashboard`

## 6) How to Interact with the Dashboard

- Use the date-range control first (for example last 24h, 7d, or custom period).
- Use `transaction_type` drop-down to focus on one segment.
- Click a bar/line point/category in a chart to cross-filter other charts.
- Use chart-level menu (**⋮**) to sort or inspect chart data as a table.
- Click blank canvas area to clear single-click chart filters.
- Use **Reset** (top-right) to clear all active interactions and return to default view.

## 7) Optional SQL Validation (BigQuery)

Before connecting Looker Studio, sanity check the KPI table:

```sql
SELECT
  event_hour_utc,
  transaction_type,
  transaction_count,
  alert_count,
  alert_rate_recomputed,
  avg_fraud_score,
  p95_fraud_score,
  max_fraud_score,
  observed_fraud_rate
FROM `fraud_analytics.mart_fraud_hourly_kpis`
ORDER BY event_hour_utc DESC, transaction_type
LIMIT 200;
```

## 8) Data Freshness Note (while Step 8 is skipped)

Because Airflow is not enabled yet, dashboard freshness depends on manual runs of:

1. Hourly batch job (`batch/hourly_batch_processing.py`)
2. BigQuery load (`batch/load_hourly_batch_to_bigquery.py`)
3. dbt mart build (`dbt run --select marts`)

Once Step 8 is added, automate those three in DAGs and keep this same dashboard.