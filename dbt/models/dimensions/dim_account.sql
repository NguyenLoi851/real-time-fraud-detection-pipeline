with origin_accounts as (
  select
    origin_account as account_id,
    event_ts,
    amount
  from {{ ref('stg_curated_scored') }}
  where origin_account is not null
),

destination_accounts as (
  select
    destination_account as account_id,
    event_ts,
    amount
  from {{ ref('stg_curated_scored') }}
  where destination_account is not null
),

all_accounts as (
  select * from origin_accounts
  union all
  select * from destination_accounts
)

select
  to_hex(md5(account_id)) as account_key,
  account_id,
  min(event_ts) as first_seen_ts,
  max(event_ts) as last_seen_ts,
  count(*) as transaction_touch_count,
  sum(amount) as total_amount_touched
from all_accounts
group by account_id