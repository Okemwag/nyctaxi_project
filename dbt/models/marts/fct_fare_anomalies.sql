{{ config(materialized='table') }}

with baseline as (
  select
    taxi_type,
    trip_month,
    avg(fare_per_mile) as avg_fare_per_mile,
    stddev_samp(fare_per_mile) as stddev_fare_per_mile
  from {{ ref('fct_taxi_trips') }}
  where fare_per_mile is not null
  group by 1, 2
),
joined as (
  select
    trips.*,
    baseline.avg_fare_per_mile,
    baseline.stddev_fare_per_mile,
    case
      when baseline.stddev_fare_per_mile is null or baseline.stddev_fare_per_mile = 0 then null
      else (trips.fare_per_mile - baseline.avg_fare_per_mile) / baseline.stddev_fare_per_mile
    end as fare_per_mile_zscore
  from {{ ref('fct_taxi_trips') }} as trips
  left join baseline
    on trips.taxi_type = baseline.taxi_type
   and trips.trip_month = baseline.trip_month
)

select
  business_trip_key,
  source_year_month,
  taxi_type,
  pickup_datetime,
  dropoff_datetime,
  pickup_location_id,
  dropoff_location_id,
  trip_distance,
  fare_amount,
  total_amount,
  fare_per_mile,
  fare_per_mile_zscore,
  has_negative_fare,
  has_invalid_distance,
  has_temporal_anomaly,
  has_invalid_zone,
  has_null_key,
  case
    when has_negative_fare then 'negative_fare'
    when has_invalid_distance then 'invalid_distance'
    when has_temporal_anomaly then 'pickup_after_dropoff'
    when has_invalid_zone then 'invalid_zone'
    when has_null_key then 'null_business_key'
    when abs(fare_per_mile_zscore) >= 3 then 'fare_rate_outlier'
    else 'other'
  end as anomaly_type
from joined
where has_negative_fare
   or has_invalid_distance
   or has_temporal_anomaly
   or has_invalid_zone
   or has_null_key
   or abs(fare_per_mile_zscore) >= 3

