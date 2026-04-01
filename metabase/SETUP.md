# Metabase Setup

Use Metabase at `http://localhost:3000` after the stack is running.

## Database Connection

Create a PostgreSQL connection with:

- Host: `postgres` when using the Metabase container network, or `localhost` from your host machine
- Port: `5432`
- Database: `nyctaxi`
- Username: `warehouse`
- Password: `warehouse`

If you connect from the Metabase UI running in Docker, use `postgres` as the hostname.

## Recommended Dashboards

Build these dashboards from the SQL in [dashboard_queries.sql](/home/okemwag/DataProjects/nyxtaxi/metabase/dashboard_queries.sql):

1. `Trip Volume and Revenue`
2. `Fare Anomalies`
3. `Peak Pickup Zones`
4. `Pipeline Monitoring`
5. `Source vs Warehouse Reconciliation`

## Suggested Visuals

- `Trip Volume and Revenue`: line chart by `trip_month`, split by `taxi_type`
- `Fare Anomalies`: table with filters on `anomaly_type`, `pickup_datetime`, `taxi_type`
- `Peak Pickup Zones`: bar chart by `pickup_zone` or heatmap by `pickup_hour`
- `Pipeline Monitoring`: KPI cards for success rate, freshness lag, duplicate rate, runtime
- `Source vs Warehouse Reconciliation`: table with conditional formatting on `source_variance`

## Useful Gold Tables

- `gold.fct_monthly_metrics`
- `gold.fct_zone_hourly_metrics`
- `gold.fct_fare_anomalies`
- `gold.ops_pipeline_monitoring`
- `gold.ops_monthly_source_reconciliation`

