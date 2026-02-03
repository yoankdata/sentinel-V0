{{ config(materialized='view') }}

with src as (
  select
    observed_at_utc,
    city,
    country,
    temp_c,
    humidity_pct,
    pressure_hpa,
    wind_speed_ms,
    fetched_at_utc
  from {{ source('sentinel_silver', 'weather_observations') }}
),

status as (
  select
    *,
    case
      when observed_at_utc is null then 'KO'
      when temp_c is null then 'KO'
      when temp_c < -10 or temp_c > 60 then 'KO'
      when humidity_pct is null then 'KO'
      when humidity_pct < 0 or humidity_pct > 100 then 'KO'
      when fetched_at_utc is null then 'KO'
      when fetched_at_utc < timestamp_sub(current_timestamp(), interval 2 day) then 'KO'
      else 'OK'
    end as DATA_STATUS
  from src
)

select
  observed_at_utc,
  city,
  country,
  temp_c,
  humidity_pct,
  pressure_hpa,
  wind_speed_ms,
  fetched_at_utc,
  DATA_STATUS
from status
