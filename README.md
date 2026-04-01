# NYC Taxi Lakehouse Pipeline

Batch lakehouse scaffold for monthly NYC TLC taxi trip files with bronze, silver, and gold layers.

## Stack

- Docker Compose for local orchestration
- MinIO as the raw object store
- PostgreSQL as the warehouse
- Python for ingestion, normalization, and idempotent loads
- dbt for incremental marts and monitoring models
- Airflow for scheduling and backfills
- Metabase for dashboarding

## Architecture

- Bronze: raw TLC parquet files copied into MinIO at `bronze/<taxi_type>/year_month=<YYYY-MM>/`
- Silver: normalized and deduplicated trips loaded into partitioned Postgres tables in `silver`
- Gold: dbt models in `gold` for fact tables, dimensions, monitoring, and dashboard-friendly aggregates

Operational metadata lands in `ops.batch_runs`, which tracks source row counts, duplicate rates, null-key rates, schema drift, and runtime metrics.

## Edge Cases Covered

- Negative fares or impossible distances are flagged during silver load
- Pickup timestamps after dropoff timestamps are flagged
- Null or invalid zone IDs are flagged and routed through unknown zone handling in dimensions
- Vendor field changes across months are normalized through an alias map
- Duplicate trip rows are deduplicated on a business key during silver load
- Unexpected schema drift is recorded at the batch level in `ops.batch_runs.schema_drift_columns`
- Late-arriving corrections are handled with upserts on the silver business key

## Quick Start

1. Copy the environment template.

   ```bash
   cp .env.example .env
   ```

2. Start the stack.

   ```bash
   docker compose up airflow-init minio-init
   docker compose up -d postgres minio airflow-webserver airflow-scheduler dbt metabase
   ```

3. Run an initial monthly load.

   ```bash
   docker compose run --rm airflow-webserver python -m nyctaxi.cli run-month --taxi-type yellow --year-month 2024-01
   ```

4. Run a historical backfill when needed.

   ```bash
   docker compose run --rm airflow-webserver python -m nyctaxi.cli run-range --taxi-type yellow --start-month 2024-01 --end-month 2024-06
   ```

5. Build marts and tests.

   ```bash
   docker compose exec dbt dbt run --project-dir /opt/project/dbt --profiles-dir /opt/project/dbt
   docker compose exec dbt dbt test --project-dir /opt/project/dbt --profiles-dir /opt/project/dbt
   ```

6. Operate backfills through Airflow once the stack is live.

   Examples are in [airflow/OPERATIONS.md](/home/okemwag/DataProjects/nyxtaxi/airflow/OPERATIONS.md).

## Airflow

- UI: `http://localhost:8080`
- Default credentials: `admin / admin`
- DAG: `nyc_taxi_lakehouse`
- Schedule: monthly on day 2 at 06:00 UTC
- Runtime parameter override:
  `year_month=YYYY-MM`
  `start_month=YYYY-MM`
  `end_month=YYYY-MM`
  `taxi_type=yellow|green`

The DAG bootstraps warehouse schemas, refreshes the taxi zone lookup, ingests one month or an inclusive month range, runs `dbt source freshness`, `dbt run`, and `dbt test`, and includes retries plus execution timeouts for operational use.

## MinIO

- API: `http://localhost:9000`
- Console: `http://localhost:9001`
- Default credentials: `minio / minio123`
- Bucket: `${RAW_BUCKET}`

## Metabase

- UI: `http://localhost:3000`
- Starter SQL questions are in [metabase/dashboard_queries.sql](/home/okemwag/DataProjects/nyxtaxi/metabase/dashboard_queries.sql)
- Setup steps are in [metabase/SETUP.md](/home/okemwag/DataProjects/nyxtaxi/metabase/SETUP.md)
- Dashboard layout guidance is in [metabase/DASHBOARDS.md](/home/okemwag/DataProjects/nyxtaxi/metabase/DASHBOARDS.md)

Recommended dashboards:

- Trip volume and revenue by month
- Fare anomaly review
- Peak pickup zones by hour
- Pipeline health and freshness
- Source totals vs warehouse totals

## Project Layout

- [docker-compose.yml](/home/okemwag/DataProjects/nyxtaxi/docker-compose.yml)
- [airflow/dags/nyc_taxi_pipeline.py](/home/okemwag/DataProjects/nyxtaxi/airflow/dags/nyc_taxi_pipeline.py)
- [ingest/src/nyctaxi/pipeline.py](/home/okemwag/DataProjects/nyxtaxi/ingest/src/nyctaxi/pipeline.py)
- [ingest/src/nyctaxi/warehouse.py](/home/okemwag/DataProjects/nyxtaxi/ingest/src/nyctaxi/warehouse.py)
- [dbt/models/marts/fct_monthly_metrics.sql](/home/okemwag/DataProjects/nyxtaxi/dbt/models/marts/fct_monthly_metrics.sql)
- [dbt/models/marts/ops_pipeline_monitoring.sql](/home/okemwag/DataProjects/nyxtaxi/dbt/models/marts/ops_pipeline_monitoring.sql)

## Pipeline Metrics

The dbt gold layer exposes:

- ingest success rate
- rows ingested per file
- duplicate rate
- null-rate on key columns
- average pipeline runtime
- freshness lag
- trip count by month vs source totals

## Performance

- Silver loads now upsert in configurable chunks.
- Set `NYCTAXI_LOAD_CHUNK_SIZE` to tune copy/upsert batch size for larger backfills.
- Default chunk size is `250000` rows.

## Data Quality Checks

dbt tests now cover:

- accepted values for pipeline batch status and trip quality status
- batch accounting: `source_rows = loaded_rows + duplicate_rows` for successful runs
- unique fact grain on `(business_trip_key, source_year_month)`
- reconciliation of latest successful batch counts to the warehouse snapshot
