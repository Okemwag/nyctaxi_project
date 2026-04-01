{{ config(materialized='table') }}

with monitoring as (
  select *
  from {{ ref('ops_pipeline_monitoring') }}
),
reconciliation as (
  select *
  from {{ ref('ops_monthly_source_reconciliation') }}
),
monitoring_alerts as (
  select
    taxi_type,
    null::text as year_month,
    case
      when ingest_success_rate < 0.99 then 'error'
      when freshness_lag_hours > 36 then 'error'
      when duplicate_rate > 0.05 then 'warn'
      when null_rate_key_columns > 0.001 then 'warn'
      else null
    end as severity,
    case
      when ingest_success_rate < 0.99 then 'ingest_success_rate_below_threshold'
      when freshness_lag_hours > 36 then 'freshness_lag_above_threshold'
      when duplicate_rate > 0.05 then 'duplicate_rate_above_threshold'
      when null_rate_key_columns > 0.001 then 'null_key_rate_above_threshold'
      else null
    end as alert_code,
    case
      when ingest_success_rate < 0.99 then format('ingest_success_rate=%.4f', ingest_success_rate)
      when freshness_lag_hours > 36 then format('freshness_lag_hours=%.2f', freshness_lag_hours)
      when duplicate_rate > 0.05 then format('duplicate_rate=%.4f', duplicate_rate)
      when null_rate_key_columns > 0.001 then format('null_rate_key_columns=%.4f', null_rate_key_columns)
      else null
    end as alert_context
  from monitoring
),
reconciliation_alerts as (
  select
    taxi_type,
    year_month,
    case
      when loaded_variance <> 0 then 'error'
      when source_variance <> 0 then 'warn'
      else null
    end as severity,
    case
      when loaded_variance <> 0 then 'loaded_reconciliation_mismatch'
      when source_variance <> 0 then 'source_reconciliation_mismatch'
      else null
    end as alert_code,
    format(
      'expected_loaded_rows=%s warehouse_rows=%s source_rows=%s',
      expected_loaded_rows,
      warehouse_rows,
      source_rows
    ) as alert_context
  from reconciliation
)

select
  taxi_type,
  year_month,
  severity,
  alert_code,
  alert_context,
  now() as evaluated_at
from monitoring_alerts
where severity is not null

union all

select
  taxi_type,
  year_month,
  severity,
  alert_code,
  alert_context,
  now() as evaluated_at
from reconciliation_alerts
where severity is not null

