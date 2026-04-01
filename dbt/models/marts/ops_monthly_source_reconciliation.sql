{{ config(materialized='table') }}

with source_totals as (
  select
    taxi_type,
    year_month,
    sum(source_rows) as source_rows
  from {{ ref('stg_batch_runs') }}
  where status = 'success'
  group by 1, 2
),
warehouse_totals as (
  select
    taxi_type,
    source_year_month as year_month,
    count(*) as warehouse_rows
  from {{ ref('fct_taxi_trips') }}
  group by 1, 2
)

select
  source_totals.taxi_type,
  source_totals.year_month,
  source_totals.source_rows,
  coalesce(warehouse_totals.warehouse_rows, 0) as warehouse_rows,
  coalesce(warehouse_totals.warehouse_rows, 0) - source_totals.source_rows as source_variance
from source_totals
left join warehouse_totals
  on source_totals.taxi_type = warehouse_totals.taxi_type
 and source_totals.year_month = warehouse_totals.year_month

