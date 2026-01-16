{{ config(materialized='table') }}

select *
from {{ ref('fct_trips') }}
where
  dropoff_ts >= pickup_ts
  and trip_distance > 0
  and total_amount >= 0
  and duration_minutes between 0 and 24*60