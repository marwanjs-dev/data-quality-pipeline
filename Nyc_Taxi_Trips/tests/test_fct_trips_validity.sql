select *
from {{ ref('fct_trips') }}
where
  pickup_ts is null
  or dropoff_ts is null
  or dropoff_ts < pickup_ts
  or trip_distance is null
  or trip_distance <= 0
  or total_amount is null
  or total_amount < 0
  or (extract(epoch from (dropoff_ts - pickup_ts)) / 60.0) < 0
  or (extract(epoch from (dropoff_ts - pickup_ts)) / 60.0) > 24*60
