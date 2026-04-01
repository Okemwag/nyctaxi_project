-- Trip volume and revenue by month
select
  trip_month,
  taxi_type,
  trip_count,
  total_revenue,
  avg_total_amount,
  flagged_trip_count
from gold.fct_monthly_metrics
order by trip_month, taxi_type;

-- Fare anomaly review
select
  pickup_datetime,
  taxi_type,
  anomaly_type,
  pickup_location_id,
  dropoff_location_id,
  trip_distance,
  total_amount,
  fare_per_mile_zscore
from gold.fct_fare_anomalies
order by pickup_datetime desc;

-- Peak pickup zones by hour
select
  pickup_date,
  pickup_hour,
  taxi_type,
  pickup_borough,
  pickup_zone,
  trip_count,
  total_revenue
from gold.fct_zone_hourly_metrics
order by pickup_date desc, trip_count desc;

-- Pipeline monitoring
select
  taxi_type,
  ingest_success_rate,
  avg_rows_ingested_per_file,
  duplicate_rate,
  null_rate_key_columns,
  average_pipeline_runtime_seconds,
  freshness_lag_hours
from gold.ops_pipeline_monitoring;

-- Source totals vs warehouse totals
select
  year_month,
  taxi_type,
  source_rows,
  warehouse_rows,
  source_variance
from gold.ops_monthly_source_reconciliation
order by year_month desc, taxi_type;

