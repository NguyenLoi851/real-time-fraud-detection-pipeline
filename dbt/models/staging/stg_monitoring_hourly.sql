select
  cast(batch_hour_utc as timestamp) as event_hour_utc,
  cast(type as string) as transaction_type,
  cast(txn_count as int64) as transaction_count,
  cast(alert_count as int64) as alert_count,
  cast(avg_fraud_score as float64) as avg_fraud_score,
  cast(p95_fraud_score as float64) as p95_fraud_score,
  cast(observed_fraud_rate as float64) as observed_fraud_rate,
  cast(alert_rate as float64) as alert_rate
from {{ source('fraud_raw', 'monitoring_hourly') }}