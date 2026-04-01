select
  batch_id,
  taxi_type,
  year_month,
  source_rows,
  loaded_rows,
  duplicate_rows
from {{ ref('stg_batch_runs') }}
where status = 'success'
  and source_rows <> loaded_rows + duplicate_rows

