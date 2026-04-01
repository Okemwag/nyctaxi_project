{{ config(
    materialized='incremental',
    unique_key=['business_trip_key', 'source_year_month'],
    on_schema_change='sync_all_columns'
) }}

with base as (
  select *
  from {{ ref('stg_taxi_trips') }}
  {% if is_incremental() %}
    where updated_at >= (
      select coalesce(max(updated_at), timestamp '1900-01-01')
      from {{ this }}
    )
  {% endif %}
)

select
  base.business_trip_key,
  base.source_year_month,
  {{ surrogate_key(["base.business_trip_key", "base.source_year_month"]) }} as trip_sk,
  vendor.vendor_sk,
  coalesce(pickup_zone.zone_sk, {{ surrogate_key(["0"]) }}) as pickup_zone_sk,
  coalesce(dropoff_zone.zone_sk, {{ surrogate_key(["0"]) }}) as dropoff_zone_sk,
  base.taxi_type,
  base.source_file_name,
  base.pickup_datetime,
  base.dropoff_datetime,
  base.trip_month,
  base.pickup_date,
  base.pickup_hour,
  base.passenger_count,
  base.trip_distance,
  base.rate_code_id,
  base.store_and_fwd_flag,
  base.pickup_location_id,
  base.dropoff_location_id,
  base.payment_type,
  base.fare_amount,
  base.extra,
  base.mta_tax,
  base.tip_amount,
  base.tolls_amount,
  base.improvement_surcharge,
  base.total_amount,
  base.congestion_surcharge,
  base.airport_fee,
  base.trip_type,
  base.ehail_fee,
  base.fare_per_mile,
  base.has_negative_fare,
  base.has_invalid_distance,
  base.has_temporal_anomaly,
  base.has_invalid_zone,
  base.has_null_key,
  base.has_schema_drift,
  base.quality_status,
  base.loaded_at,
  base.updated_at
from base
left join {{ ref('dim_vendor') }} as vendor
  on base.taxi_type = vendor.taxi_type
 and base.vendor_id = vendor.vendor_id
left join {{ ref('dim_zone') }} as pickup_zone
  on coalesce(base.pickup_location_id, 0) = pickup_zone.location_id
left join {{ ref('dim_zone') }} as dropoff_zone
  on coalesce(base.dropoff_location_id, 0) = dropoff_zone.location_id

