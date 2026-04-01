{{ config(materialized='table') }}

select
  taxi_type,
  trip_month,
  count(*) as trip_count,
  sum(total_amount) as total_revenue,
  avg(total_amount) as avg_total_amount,
  avg(trip_distance) as avg_trip_distance,
  avg(fare_per_mile) as avg_fare_per_mile,
  sum(case when quality_status = 'flagged' then 1 else 0 end) as flagged_trip_count,
  sum(case when has_negative_fare then 1 else 0 end) as negative_fare_count,
  sum(case when has_invalid_distance then 1 else 0 end) as invalid_distance_count,
  sum(case when has_temporal_anomaly then 1 else 0 end) as temporal_anomaly_count,
  sum(case when has_invalid_zone then 1 else 0 end) as invalid_zone_count
from {{ ref('fct_taxi_trips') }}
group by 1, 2

