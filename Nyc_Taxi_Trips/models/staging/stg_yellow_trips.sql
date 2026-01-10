{{ config(materialized='view') }}

select
  cast(vendorid as integer) as vendor_id,

  cast(tpep_pickup_datetime as timestamp)  as pickup_ts,
  cast(tpep_dropoff_datetime as timestamp) as dropoff_ts,

  cast(pulocationid as integer) as pu_location_id,
  cast(dolocationid as integer) as do_location_id,

  cast(passenger_count as integer) as passenger_count,
  cast(trip_distance as double precision) as trip_distance,

  cast(ratecodeid as integer) as rate_code_id,
  cast(store_and_fwd_flag as text) as store_and_fwd_flag,
  cast(payment_type as integer) as payment_type,

  cast(fare_amount as double precision) as fare_amount,
  cast(extra as double precision) as extra,
  cast(mta_tax as double precision) as mta_tax,
  cast(tip_amount as double precision) as tip_amount,
  cast(tolls_amount as double precision) as tolls_amount,
  cast(improvement_surcharge as double precision) as improvement_surcharge,
  cast(total_amount as double precision) as total_amount,

  -- These columns may or may not exist depending on month/version.
  -- If your month doesn't have one, tell me the error and we'll adjust safely.
  cast(congestion_surcharge as double precision) as congestion_surcharge,
  cast(airport_fee as double precision) as airport_fee,
  cast(cbd_congestion_fee as double precision) as cbd_congestion_fee

from {{ source('raw', 'yellow_trips') }}
