with base as (
  select
    *
  from {{ ref('stg_curated_scored') }}
),

enriched as (
  select
    to_hex(
      md5(
        concat(
          coalesce(cast(step as string), ''), '|',
          coalesce(transaction_type, ''), '|',
          coalesce(origin_account, ''), '|',
          coalesce(destination_account, ''), '|',
          coalesce(cast(event_ts as string), ''), '|',
          coalesce(cast(source_partition as string), ''), '|',
          coalesce(cast(source_offset as string), '')
        )
      )
    ) as transaction_key,
    event_ts,
    event_hour_utc,
    event_date,
    step,
    transaction_type,
    to_hex(md5(transaction_type)) as transaction_type_key,
    origin_account,
    to_hex(md5(origin_account)) as origin_account_key,
    destination_account,
    to_hex(md5(destination_account)) as destination_account_key,
    amount,
    old_balance_origin,
    new_balance_origin,
    old_balance_destination,
    new_balance_destination,
    velocity_5min,
    balance_change_ratio,
    is_new_merchant,
    origin_balance_delta,
    destination_balance_delta,
    fraud_score,
    predicted_is_fraud,
    is_alert,
    is_fraud_label,
    is_label_available,
    label_delay_hours,
    source_topic,
    source_partition,
    source_offset
  from base
)

select * from enriched