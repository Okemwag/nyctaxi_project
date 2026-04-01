{{ config(materialized='table') }}

select
  taxi_type,
  count(*) as batch_count,
  avg(case when status = 'success' then 1.0 else 0.0 end) as ingest_success_rate,
  avg(source_rows) as avg_rows_ingested_per_file,
  avg(case when source_rows > 0 then duplicate_rows::float / source_rows else 0 end) as duplicate_rate,
  avg(case when source_rows > 0 then null_key_rows::float / source_rows else 0 end) as null_rate_key_columns,
  avg(runtime_seconds) as average_pipeline_runtime_seconds,
  max(finished_at) as latest_success_at,
  extract(epoch from (now() - max(finished_at))) / 3600.0 as freshness_lag_hours
from {{ ref('stg_batch_runs') }}
where status in ('success', 'failed')
group by 1

