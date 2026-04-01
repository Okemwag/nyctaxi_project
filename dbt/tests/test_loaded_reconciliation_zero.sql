select
  taxi_type,
  year_month,
  expected_loaded_rows,
  warehouse_rows,
  loaded_variance
from {{ ref('ops_monthly_source_reconciliation') }}
where loaded_variance <> 0
