{{ config(materialized='table') }}

with observed as (
  select pickup_location_id as location_id from {{ ref('stg_taxi_trips') }}
  union
  select dropoff_location_id as location_id from {{ ref('stg_taxi_trips') }}
),
lookup as (
  select
    location_id,
    borough,
    zone,
    service_zone,
    true as is_known_zone
  from {{ ref('stg_taxi_zones') }}
),
unknowns as (
  select distinct
    observed.location_id,
    'Unknown' as borough,
    'Unknown' as zone,
    'Unknown' as service_zone,
    false as is_known_zone
  from observed
  left join lookup using (location_id)
  where observed.location_id is not null
    and lookup.location_id is null
),
all_zones as (
  select * from lookup
  union all
  select * from unknowns
  union all
  select
    0 as location_id,
    'Unknown' as borough,
    'Unknown' as zone,
    'Unknown' as service_zone,
    false as is_known_zone
)

select
  {{ surrogate_key(["location_id"]) }} as zone_sk,
  location_id,
  borough,
  zone,
  service_zone,
  is_known_zone
from all_zones

