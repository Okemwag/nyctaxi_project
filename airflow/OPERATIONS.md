# Airflow Operations

Use Airflow as the primary operational entrypoint once the stack is running.

## DAG

- DAG ID: `nyc_taxi_lakehouse`
- Schedule: monthly on day 2 at `06:00 UTC`

## Manual Backfill

Trigger the DAG from the UI or CLI with:

```json
{
  "taxi_type": "yellow",
  "start_month": "2024-01",
  "end_month": "2024-06"
}
```

For a single month:

```json
{
  "taxi_type": "yellow",
  "year_month": "2024-07"
}
```

## Runtime Flow

1. Bootstrap warehouse schemas and refresh zones.
2. Ingest one or more months.
3. Run `dbt source freshness`.
4. Run `dbt run`.
5. Run `dbt test`.
6. Evaluate alert thresholds from `gold.ops_alerts`.

## Alerts

The DAG fails when `gold.ops_alerts` contains an `error` severity record.

Current thresholds:

- ingest success rate below `0.99`
- freshness lag above `36` hours
- reconciliation mismatch where `loaded_variance <> 0`

Warnings are retained in `gold.ops_alerts` for dashboard visibility but do not fail the DAG.

