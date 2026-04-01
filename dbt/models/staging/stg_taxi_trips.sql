{{ config(materialized='view') }}

select
  business_trip_key,
  source_year_month,
  row_fingerprint,
  batch_id,
  taxi_type,
  source_file_name,
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  cast(date_trunc('month', pickup_datetime) as date) as trip_month,
  cast(pickup_datetime as date) as pickup_date,
  extract(hour from pickup_datetime) as pickup_hour,
  passenger_count,
  trip_distance,
  rate_code_id,
  store_and_fwd_flag,
  pickup_location_id,
  dropoff_location_id,
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
  trip_type,
  ehail_fee,
  case
    when trip_distance > 0 then total_amount / nullif(trip_distance, 0)
    else null
  end as fare_per_mile,
  has_negative_fare,
  has_invalid_distance,
  has_temporal_anomaly,
  has_invalid_zone,
  has_null_key,
  has_schema_drift,
  quality_status,
  loaded_at,
  updated_at
from {{ source('silver', 'taxi_trips') }}

