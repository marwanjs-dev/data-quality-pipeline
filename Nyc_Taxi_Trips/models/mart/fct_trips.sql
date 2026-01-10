{{ config(materialized='table') }}

with t as (
  select * from {{ ref('stg_yellow_trips') }}
),

enriched as (
  select
    -- just to detect duplicates
    md5(
      coalesce(cast(vendor_id as text), '') || '|' ||
      coalesce(cast(pickup_ts as text), '') || '|' ||
      coalesce(cast(dropoff_ts as text), '') || '|' ||
      coalesce(cast(pu_location_id as text), '') || '|' ||
      coalesce(cast(do_location_id as text), '') || '|' ||
      coalesce(cast(total_amount as text), '') || '|' ||
      coalesce(cast(trip_distance as text), '')
    ) as trip_id,

    vendor_id,
    pickup_ts,
    dropoff_ts,
    pu_location_id,
    do_location_id,

    passenger_count,
    trip_distance,
    rate_code_id,
    store_and_fwd_flag,
    payment_type,

    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee,
    cbd_congestion_fee,

    (extract(epoch from (dropoff_ts - pickup_ts)) / 60.0) as duration_minutes,
    date_trunc('hour', pickup_ts) as pickup_hour

  from t
)

select * from enriched
