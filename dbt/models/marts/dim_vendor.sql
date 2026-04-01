{{ config(materialized='table') }}

with distinct_vendors as (
  select distinct
    taxi_type,
    vendor_id
  from {{ ref('stg_taxi_trips') }}
)

select
  {{ surrogate_key(["taxi_type", "vendor_id"]) }} as vendor_sk,
  taxi_type,
  vendor_id,
  case vendor_id
    when 1 then 'Creative Mobile Technologies'
    when 2 then 'VeriFone'
    when 4 then 'Via'
    else 'Unknown'
  end as vendor_name
from distinct_vendors

