select
  transaction_key,
  event_ts,
  event_hour_utc,
  event_date,
  transaction_type,
  transaction_type_key,
  origin_account,
  origin_account_key,
  destination_account,
  destination_account_key,
  amount,
  fraud_score,
  predicted_is_fraud,
  is_fraud_label,
  is_label_available,
  label_delay_hours,
  source_topic,
  source_partition,
  source_offset
from {{ ref('fct_scored_transactions') }}
where is_alert = true