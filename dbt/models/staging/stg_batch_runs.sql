{{ config(materialized='view') }}

select
  batch_id,
  taxi_type,
  year_month,
  source_url,
  source_file_name,
  bronze_object_key,
  source_etag,
  source_last_modified,
  source_rows,
  loaded_rows,
  duplicate_rows,
  invalid_rows,
  null_key_rows,
  status,
  schema_drift_columns,
  error_message,
  started_at,
  finished_at,
  runtime_seconds
from {{ source('ops', 'batch_runs') }}

