with base as (
  select distinct
    event_hour_utc
  from {{ ref('stg_curated_scored') }}
  where event_hour_utc is not null
)

select
  event_hour_utc as time_hour_utc,
  extract(date from event_hour_utc) as event_date,
  extract(year from event_hour_utc) as event_year,
  extract(month from event_hour_utc) as event_month,
  extract(day from event_hour_utc) as event_day,
  extract(hour from event_hour_utc) as event_hour,
  format_timestamp('%A', event_hour_utc) as day_name,
  format_timestamp('%Y-%m-%d %H:00:00 UTC', event_hour_utc) as hour_label_utc
from base