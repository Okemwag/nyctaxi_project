{{ config(materialized='view') }}

select
  location_id,
  borough,
  zone,
  service_zone,
  updated_at
from {{ source('raw', 'taxi_zone_lookup') }}

