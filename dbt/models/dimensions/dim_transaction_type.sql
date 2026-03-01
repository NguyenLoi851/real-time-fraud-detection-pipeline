with base as (
  select distinct
    transaction_type
  from {{ ref('stg_curated_scored') }}
  where transaction_type is not null
)

select
  to_hex(md5(transaction_type)) as transaction_type_key,
  transaction_type
from base