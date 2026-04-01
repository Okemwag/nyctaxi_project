{{ config(materialized='table') }}

with latest_successful_batch as (
  select
    taxi_type,
    year_month,
    source_rows,
    loaded_rows,
    duplicate_rows,
    invalid_rows,
    null_key_rows,
    finished_at,
    row_number() over (
      partition by taxi_type, year_month
      order by finished_at desc, started_at desc, batch_id desc
    ) as batch_rank
  from {{ ref('stg_batch_runs') }}
  where status = 'success'
),
batch_totals as (
  select
    taxi_type,
    year_month,
    source_rows,
    loaded_rows as expected_loaded_rows,
    duplicate_rows,
    invalid_rows,
    null_key_rows,
    finished_at as latest_success_at
  from latest_successful_batch
  where batch_rank = 1
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
  batch_totals.taxi_type,
  batch_totals.year_month,
  batch_totals.source_rows,
  batch_totals.expected_loaded_rows,
  batch_totals.duplicate_rows,
  batch_totals.invalid_rows,
  batch_totals.null_key_rows,
  batch_totals.latest_success_at,
  coalesce(warehouse_totals.warehouse_rows, 0) as warehouse_rows,
  coalesce(warehouse_totals.warehouse_rows, 0) - batch_totals.source_rows as source_variance,
  coalesce(warehouse_totals.warehouse_rows, 0) - batch_totals.expected_loaded_rows as loaded_variance
from batch_totals
left join warehouse_totals
  on batch_totals.taxi_type = warehouse_totals.taxi_type
 and batch_totals.year_month = warehouse_totals.year_month
