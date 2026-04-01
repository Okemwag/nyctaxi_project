{{ config(materialized='table') }}

select
  trips.taxi_type,
  trips.pickup_date,
  trips.pickup_hour,
  trips.pickup_location_id,
  zones.borough as pickup_borough,
  zones.zone as pickup_zone,
  count(*) as trip_count,
  sum(trips.total_amount) as total_revenue,
  avg(trips.trip_distance) as avg_trip_distance
from {{ ref('fct_taxi_trips') }} as trips
left join {{ ref('dim_zone') }} as zones
  on trips.pickup_zone_sk = zones.zone_sk
group by 1, 2, 3, 4, 5, 6

