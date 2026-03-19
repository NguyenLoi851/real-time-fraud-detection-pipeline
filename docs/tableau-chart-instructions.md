# Tableau Presentation Chart Instructions (Fraud Pipeline)

This guide is copy-paste oriented and uses your existing dbt models:
- `mart_fraud_hourly_kpis`
- `fct_scored_transactions`
- `fct_fraud_alerts`

---

## 1) Data model mapping (what to use)

### A. Executive trend charts
Use: `mart_fraud_hourly_kpis`

Important columns:
- `event_hour_utc` (datetime)
- `transaction_type` (dimension)
- `transaction_count` (measure)
- `alert_count` (measure)
- `alert_rate_recomputed` (measure)
- `alert_rate_batch` (measure)
- `avg_fraud_score` (measure)
- `p95_fraud_score` (measure)
- `max_fraud_score` (measure)
- `observed_fraud_rate` (measure)

### B. Model behavior / score distribution
Use: `fct_scored_transactions`

Important columns:
- `event_ts`, `event_hour_utc`, `event_date`
- `transaction_type`
- `amount`
- `fraud_score`
- `predicted_is_fraud`
- `is_alert`
- `is_fraud_label`
- `is_label_available`

### C. Alert operations view
Use: `fct_fraud_alerts`

Important columns:
- `event_ts`, `event_hour_utc`, `event_date`
- `transaction_type`
- `amount`
- `fraud_score`
- `predicted_is_fraud`
- `is_fraud_label`
- `is_label_available`

---

## 2) Tableau connection setup (recommended)

### Option A (quickest): connect directly to `mart_fraud_hourly_kpis`
Use this for trend-focused presentation.

### Option B (full dashboard): connect all three tables
If your Tableau connection supports joins/relationships, relate tables on:
- `fct_scored_transactions.transaction_key = fct_fraud_alerts.transaction_key` (for drill analysis)
- Keep `mart_fraud_hourly_kpis` as a separate logical table for KPI trend sheets.

---

## 3) Calculated fields to create in Tableau (copy/paste)

Create these in each relevant datasource.

### Shared formatting
- Set all `%` outputs to Percentage format with 2 decimal places.

```tableau
// Predicted Alert %
IF COUNT([transaction_key]) = 0 THEN 0
ELSE SUM(IIF([is_alert],1,0)) / COUNT([transaction_key])
END
```

```tableau
// Real Fraud Transactions (labeled)
SUM(IIF([is_label_available] AND [is_fraud_label],1,0))
```

```tableau
// Alerted Transactions
SUM(IIF([is_alert],1,0))
```

```tableau
// Real Fraud Alerted Count
SUM(IIF([is_label_available] AND [is_fraud_label] = 1 AND [is_alert],1,0))
```

```tableau
// Real Fraud Not Alerted Count
SUM(IIF([is_label_available] AND [is_fraud_label] = 1 AND NOT [is_alert],1,0))
```

```tableau
// % of Real Fraud Alerted
IF SUM(IIF([is_label_available] AND [is_fraud_label] = 1,1,0)) = 0 THEN 0
ELSE SUM(IIF([is_label_available] AND [is_fraud_label] = 1 AND [is_alert],1,0))
  / SUM(IIF([is_label_available] AND [is_fraud_label] = 1,1,0))
END
```

```tableau
// % of Real Fraud Not Alerted
IF SUM(IIF([is_label_available] AND [is_fraud_label] = 1,1,0)) = 0 THEN 0
ELSE SUM(IIF([is_label_available] AND [is_fraud_label] = 1 AND NOT [is_alert],1,0))
  / SUM(IIF([is_label_available] AND [is_fraud_label] = 1,1,0))
END
```

```tableau
// Label Coverage % (alerts)
IF COUNT([transaction_key]) = 0 THEN 0
ELSE SUM(IIF([is_label_available],1,0)) / COUNT([transaction_key])
END
```

---

## 4) Chart-by-chart build instructions

## Chart 1 — Fraud Trend (Bar Chart)
**Purpose:** Show monthly alert trend.

- Data source: `mart_fraud_hourly_kpis`
- Chart type: **Bar**
- Columns (X-axis): `event_hour_utc` (continuous month)
- Rows (Y-axis): `SUM(alert_count)`
- Color: `transaction_type`
- Filter: date range (last 7 days / 30 days)

Optional second version:
- Replace Y-axis with calculated `Alert Rate %`.

---

## Chart 2 — Volume vs Alerts (Dual-Axis Line)
**Purpose:** Compare traffic and alert volume.

- Data source: `mart_fraud_hourly_kpis`
- Chart type: **Dual-axis line**
- X-axis: `event_hour_utc`
- Y-axis #1: `SUM(transaction_count)`
- Y-axis #2: `SUM(alert_count)`
- Right-click second measure -> Dual Axis
- Keep independent axis scales unless your audience requires synchronized scale.

Formatting:
- Transaction line: blue
- Alert line: red
- Add tooltips with both values and `Alert Rate %`

---

## Chart 3 — Top Risky Accounts (Horizontal Bar)
**Purpose:** Identify concentrated risk entities.

- Data source: `fct_fraud_alerts`
- Chart type: **Horizontal Bar**
- Rows (Y-axis): `origin_account` (or `destination_account`)
- Columns (X-axis): `COUNT(transaction_key)`
- Filter: Top N = 10 (by `COUNT(transaction_key)`)
- Optional color: AVG(`fraud_score`)

---

## Chart 4 — Transaction Type (Pie Chart)
**Purpose:** Transaction Type.

- Data source: `fct_fraud_alerts`
- Chart type: **Pie**
- Angle: `COUNT(transaction_key)`
- Color: `Transaction Type`

---

## Chart 5 — KPI / Statistics Tiles
**Purpose:** Executive summary metrics.

Create a text table or individual KPI sheets:
- `COUNT([transaction_key])` as **Total Transactions** (from `fct_scored_transactions`)
- `Real Fraud Transactions (labeled)` as **Total Real Fraud Transactions**
- `Alerted Transactions` as **Transactions Alerted**
- `% of Real Fraud Alerted` as **Real Fraud Alerted %**
- `% of Real Fraud Not Alerted` as **Real Fraud Not Alerted %**

Validation check (recommended):
- `% of Real Fraud Alerted + % of Real Fraud Not Alerted` should be approximately `100%` (for the same filter context).

---

## 5) Presentation dashboard layout (recommended)

- Top row: KPI tiles (5 to 6)
- Middle row: Chart 1 (Fraud Trend) + Chart 2 (Volume vs Alerts)
- Bottom row: Chart 3 (Stacked Bar) + Chart 5 (Heatmap) + Chart 4 (Top Accounts)
- Optional final slide: Chart 6 Pie + action notes

Dashboard filters (apply to all):
- Date range
- Transaction type
- Risk bucket / severity

---

## 6) Final checklist before presentation

- Verify timezone labels (UTC vs local business timezone).
- Lock date filters to the exact reporting window.
- Ensure percentage fields are formatted as `%`.
- Use consistent colors for risk semantics (low -> high).
- Add tooltip definitions for `fraud_score`, `is_alert`, and `is_fraud_label`.
- Validate totals: sum of stacked bars should match KPI totals.
