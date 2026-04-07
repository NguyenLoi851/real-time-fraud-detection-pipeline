# PaySim Dataset Column Guide

## Purpose

Define dataset placement and column semantics used by training, streaming, and batch jobs.

## Prerequisites

See shared setup: [../docs/prerequisites.md](../docs/prerequisites.md)

## Dataset Setup

Kaggle dataset URL: https://www.kaggle.com/datasets/ealaxi/paysim1

1. Download the dataset from the URL above.
2. Unzip the downloaded file.
3. Rename the CSV file to `transaction_log.csv`.
4. Put it in this `data/` folder.

Expected path:

`data/transaction_log.csv`

## Sample Row

```csv
1,PAYMENT,1060.31,C429214117,1089.0,28.69,M1591654462,0.0,0.0,0,0
```

## Column Explanations

### step
Maps a unit of time in the real world. In this dataset, 1 step is 1 hour. Total steps: 744 (30-day simulation).

### type
Transaction type: `CASH-IN`, `CASH-OUT`, `DEBIT`, `PAYMENT`, `TRANSFER`.

### amount
Amount of the transaction in local currency.

### nameOrig
Customer who started the transaction.

### oldbalanceOrg
Initial balance of the origin account before the transaction.

### newbalanceOrig
New balance of the origin account after the transaction.

### nameDest
Customer who is the recipient of the transaction.

### oldbalanceDest
Initial balance of the recipient account before the transaction.
Note: there is no information for customers that start with `M` (Merchants).

### newbalanceDest
New balance of the recipient account after the transaction.
Note: there is no information for customers that start with `M` (Merchants).

### isFraud
Indicates transactions made by fraudulent agents inside the simulation.
In this dataset, fraud behavior aims to take control of customer accounts, transfer funds out, and then cash out of the system.

### isFlaggedFraud
Indicates whether a transaction was flagged as illegal by the business rule for massive transfers.
In this dataset, an illegal attempt is a transfer greater than `200000` in a single transaction.

## Important Note for Modeling

Transactions detected as fraud are cancelled. For fraud detection modeling, these columns should not be used as predictive features:

- `oldbalanceOrg`
- `newbalanceOrig`
- `oldbalanceDest`
- `newbalanceDest`

For end-to-end execution order, see [../docs/runbook-local.md](../docs/runbook-local.md) and [../docs/runbook-cloud.md](../docs/runbook-cloud.md).
