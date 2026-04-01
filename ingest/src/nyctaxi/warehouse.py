from __future__ import annotations

import math
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

import polars as pl
import psycopg
from psycopg import sql

from nyctaxi.models import BUSINESS_KEY_COLUMNS, FINGERPRINT_COLUMNS


WAREHOUSE_DDL = """
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS ops.batch_runs (
    batch_id UUID PRIMARY KEY,
    taxi_type TEXT NOT NULL,
    year_month TEXT NOT NULL,
    source_url TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    bronze_object_key TEXT NOT NULL,
    source_etag TEXT,
    source_last_modified TIMESTAMPTZ,
    source_rows INTEGER NOT NULL,
    loaded_rows INTEGER NOT NULL DEFAULT 0,
    duplicate_rows INTEGER NOT NULL DEFAULT 0,
    invalid_rows INTEGER NOT NULL DEFAULT 0,
    null_key_rows INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    schema_drift_columns TEXT[] NOT NULL DEFAULT '{}',
    error_message TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    runtime_seconds DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_ops_batch_runs_year_month
    ON ops.batch_runs (taxi_type, year_month, status);

CREATE TABLE IF NOT EXISTS raw.taxi_zone_lookup (
    location_id INTEGER PRIMARY KEY,
    borough TEXT,
    zone TEXT,
    service_zone TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.taxi_trips (
    business_trip_key TEXT NOT NULL,
    source_year_month TEXT NOT NULL,
    row_fingerprint TEXT NOT NULL,
    batch_id UUID NOT NULL,
    taxi_type TEXT NOT NULL,
    source_file_name TEXT NOT NULL,
    vendor_id INTEGER,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    passenger_count DOUBLE PRECISION,
    trip_distance DOUBLE PRECISION,
    rate_code_id INTEGER,
    store_and_fwd_flag TEXT,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    payment_type INTEGER,
    fare_amount DOUBLE PRECISION,
    extra DOUBLE PRECISION,
    mta_tax DOUBLE PRECISION,
    tip_amount DOUBLE PRECISION,
    tolls_amount DOUBLE PRECISION,
    improvement_surcharge DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    congestion_surcharge DOUBLE PRECISION,
    airport_fee DOUBLE PRECISION,
    trip_type INTEGER,
    ehail_fee DOUBLE PRECISION,
    has_negative_fare BOOLEAN NOT NULL DEFAULT FALSE,
    has_invalid_distance BOOLEAN NOT NULL DEFAULT FALSE,
    has_temporal_anomaly BOOLEAN NOT NULL DEFAULT FALSE,
    has_invalid_zone BOOLEAN NOT NULL DEFAULT FALSE,
    has_null_key BOOLEAN NOT NULL DEFAULT FALSE,
    has_schema_drift BOOLEAN NOT NULL DEFAULT FALSE,
    quality_status TEXT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (business_trip_key, source_year_month)
) PARTITION BY LIST (source_year_month);

CREATE TABLE IF NOT EXISTS silver.taxi_trips_default
    PARTITION OF silver.taxi_trips DEFAULT;

CREATE INDEX IF NOT EXISTS idx_silver_taxi_trips_pickup_datetime
    ON silver.taxi_trips (pickup_datetime);

CREATE INDEX IF NOT EXISTS idx_silver_taxi_trips_quality_status
    ON silver.taxi_trips (quality_status);
"""


BASE_STAGE_COLUMNS = [
    "batch_id",
    "taxi_type",
    "source_year_month",
    "source_file_name",
    "row_number_in_file",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "rate_code_id",
    "store_and_fwd_flag",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "trip_type",
    "ehail_fee",
]

FINAL_STAGE_COLUMNS = [
    "business_trip_key",
    "row_fingerprint",
    *BASE_STAGE_COLUMNS,
    "has_negative_fare",
    "has_invalid_distance",
    "has_temporal_anomaly",
    "has_invalid_zone",
    "has_null_key",
    "has_schema_drift",
    "quality_status",
]


def ensure_warehouse(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(WAREHOUSE_DDL)


def create_month_partition(dsn: str, year_month: str) -> None:
    partition_name = f"taxi_trips_{year_month.replace('-', '_')}"
    statement = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS silver.{partition_name}
        PARTITION OF silver.taxi_trips
        FOR VALUES IN ({year_month})
        """
    ).format(
        partition_name=sql.Identifier(partition_name),
        year_month=sql.Literal(year_month),
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(statement)


def batch_already_succeeded(
    dsn: str,
    taxi_type: str,
    year_month: str,
    source_etag: str | None,
) -> bool:
    if not source_etag:
        return False

    query = """
        SELECT 1
        FROM ops.batch_runs
        WHERE taxi_type = %s
          AND year_month = %s
          AND source_etag = %s
          AND status = 'success'
        LIMIT 1
    """
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (taxi_type, year_month, source_etag))
            return cur.fetchone() is not None


def start_batch(
    dsn: str,
    batch_id: str,
    taxi_type: str,
    year_month: str,
    source_url: str,
    source_file_name: str,
    bronze_object_key: str,
    source_etag: str | None,
    source_last_modified: datetime | None,
    source_rows: int,
    schema_drift_columns: list[str],
) -> None:
    statement = """
        INSERT INTO ops.batch_runs (
            batch_id,
            taxi_type,
            year_month,
            source_url,
            source_file_name,
            bronze_object_key,
            source_etag,
            source_last_modified,
            source_rows,
            status,
            schema_drift_columns,
            started_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 'running', %s, %s)
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            statement,
            (
                batch_id,
                taxi_type,
                year_month,
                source_url,
                source_file_name,
                bronze_object_key,
                source_etag,
                source_last_modified,
                source_rows,
                schema_drift_columns,
                datetime.now(timezone.utc),
            ),
        )


def finish_batch(
    dsn: str,
    batch_id: str,
    loaded_rows: int,
    duplicate_rows: int,
    invalid_rows: int,
    null_key_rows: int,
) -> None:
    finished_at = datetime.now(timezone.utc)
    statement = """
        UPDATE ops.batch_runs
        SET loaded_rows = %s,
            duplicate_rows = %s,
            invalid_rows = %s,
            null_key_rows = %s,
            status = 'success',
            finished_at = %s,
            runtime_seconds = EXTRACT(EPOCH FROM (%s - started_at))
        WHERE batch_id = %s
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            statement,
            (
                loaded_rows,
                duplicate_rows,
                invalid_rows,
                null_key_rows,
                finished_at,
                finished_at,
                batch_id,
            ),
        )


def fail_batch(dsn: str, batch_id: str, error_message: str) -> None:
    finished_at = datetime.now(timezone.utc)
    statement = """
        UPDATE ops.batch_runs
        SET status = 'failed',
            error_message = %s,
            finished_at = %s,
            runtime_seconds = EXTRACT(EPOCH FROM (%s - started_at))
        WHERE batch_id = %s
    """
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(statement, (error_message[:4000], finished_at, finished_at, batch_id))


def upsert_zone_lookup(dsn: str, zones_df: pl.DataFrame) -> int:
    ensure_warehouse(dsn)
    row_count = zones_df.height
    if row_count == 0:
        return 0

    columns = ["location_id", "borough", "zone", "service_zone"]
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE stage_zone_lookup (
                    location_id INTEGER,
                    borough TEXT,
                    zone TEXT,
                    service_zone TEXT
                ) ON COMMIT DROP
                """
            )
            _copy_rows(cur, "stage_zone_lookup", columns, zones_df.select(columns).iter_rows())
            cur.execute(
                """
                INSERT INTO raw.taxi_zone_lookup (
                    location_id,
                    borough,
                    zone,
                    service_zone,
                    updated_at
                )
                SELECT
                    location_id,
                    borough,
                    zone,
                    service_zone,
                    NOW()
                FROM stage_zone_lookup
                ON CONFLICT (location_id) DO UPDATE
                SET borough = EXCLUDED.borough,
                    zone = EXCLUDED.zone,
                    service_zone = EXCLUDED.service_zone,
                    updated_at = NOW()
                """
            )
        conn.commit()
    return row_count


def load_trip_batch(
    dsn: str,
    batch_df: pl.DataFrame,
    schema_drift_columns: list[str],
) -> dict[str, int]:
    ensure_warehouse(dsn)
    year_month = batch_df["source_year_month"][0]
    create_month_partition(dsn, year_month)
    prepared_df, metrics = _prepare_batch_dataframe(batch_df, schema_drift_columns)

    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TEMP TABLE stage_trip_batch (
                    business_trip_key TEXT,
                    row_fingerprint TEXT,
                    batch_id UUID,
                    taxi_type TEXT,
                    source_year_month TEXT,
                    source_file_name TEXT,
                    row_number_in_file BIGINT,
                    vendor_id INTEGER,
                    pickup_datetime TIMESTAMP,
                    dropoff_datetime TIMESTAMP,
                    passenger_count DOUBLE PRECISION,
                    trip_distance DOUBLE PRECISION,
                    rate_code_id INTEGER,
                    store_and_fwd_flag TEXT,
                    pickup_location_id INTEGER,
                    dropoff_location_id INTEGER,
                    payment_type INTEGER,
                    fare_amount DOUBLE PRECISION,
                    extra DOUBLE PRECISION,
                    mta_tax DOUBLE PRECISION,
                    tip_amount DOUBLE PRECISION,
                    tolls_amount DOUBLE PRECISION,
                    improvement_surcharge DOUBLE PRECISION,
                    total_amount DOUBLE PRECISION,
                    congestion_surcharge DOUBLE PRECISION,
                    airport_fee DOUBLE PRECISION,
                    trip_type INTEGER,
                    ehail_fee DOUBLE PRECISION,
                    has_negative_fare BOOLEAN,
                    has_invalid_distance BOOLEAN,
                    has_temporal_anomaly BOOLEAN,
                    has_invalid_zone BOOLEAN,
                    has_null_key BOOLEAN,
                    has_schema_drift BOOLEAN,
                    quality_status TEXT
                ) ON COMMIT DROP
                """
            )
            _copy_rows(
                cur,
                "stage_trip_batch",
                FINAL_STAGE_COLUMNS,
                prepared_df.select(FINAL_STAGE_COLUMNS).iter_rows(),
            )
            cur.execute(
                """
                INSERT INTO silver.taxi_trips (
                    business_trip_key,
                    source_year_month,
                    row_fingerprint,
                    batch_id,
                    taxi_type,
                    source_file_name,
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    rate_code_id,
                    store_and_fwd_flag,
                    pickup_location_id,
                    dropoff_location_id,
                    payment_type,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    total_amount,
                    congestion_surcharge,
                    airport_fee,
                    trip_type,
                    ehail_fee,
                    has_negative_fare,
                    has_invalid_distance,
                    has_temporal_anomaly,
                    has_invalid_zone,
                    has_null_key,
                    has_schema_drift,
                    quality_status,
                    loaded_at,
                    updated_at
                )
                SELECT
                    business_trip_key,
                    source_year_month,
                    row_fingerprint,
                    batch_id,
                    taxi_type,
                    source_file_name,
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    rate_code_id,
                    store_and_fwd_flag,
                    pickup_location_id,
                    dropoff_location_id,
                    payment_type,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    total_amount,
                    congestion_surcharge,
                    airport_fee,
                    trip_type,
                    ehail_fee,
                    has_negative_fare,
                    has_invalid_distance,
                    has_temporal_anomaly,
                    has_invalid_zone,
                    has_null_key,
                    has_schema_drift,
                    quality_status,
                    NOW(),
                    NOW()
                FROM stage_trip_batch
                ON CONFLICT (business_trip_key, source_year_month) DO UPDATE
                SET row_fingerprint = EXCLUDED.row_fingerprint,
                    batch_id = EXCLUDED.batch_id,
                    taxi_type = EXCLUDED.taxi_type,
                    source_file_name = EXCLUDED.source_file_name,
                    vendor_id = EXCLUDED.vendor_id,
                    pickup_datetime = EXCLUDED.pickup_datetime,
                    dropoff_datetime = EXCLUDED.dropoff_datetime,
                    passenger_count = EXCLUDED.passenger_count,
                    trip_distance = EXCLUDED.trip_distance,
                    rate_code_id = EXCLUDED.rate_code_id,
                    store_and_fwd_flag = EXCLUDED.store_and_fwd_flag,
                    pickup_location_id = EXCLUDED.pickup_location_id,
                    dropoff_location_id = EXCLUDED.dropoff_location_id,
                    payment_type = EXCLUDED.payment_type,
                    fare_amount = EXCLUDED.fare_amount,
                    extra = EXCLUDED.extra,
                    mta_tax = EXCLUDED.mta_tax,
                    tip_amount = EXCLUDED.tip_amount,
                    tolls_amount = EXCLUDED.tolls_amount,
                    improvement_surcharge = EXCLUDED.improvement_surcharge,
                    total_amount = EXCLUDED.total_amount,
                    congestion_surcharge = EXCLUDED.congestion_surcharge,
                    airport_fee = EXCLUDED.airport_fee,
                    trip_type = EXCLUDED.trip_type,
                    ehail_fee = EXCLUDED.ehail_fee,
                    has_negative_fare = EXCLUDED.has_negative_fare,
                    has_invalid_distance = EXCLUDED.has_invalid_distance,
                    has_temporal_anomaly = EXCLUDED.has_temporal_anomaly,
                    has_invalid_zone = EXCLUDED.has_invalid_zone,
                    has_null_key = EXCLUDED.has_null_key,
                    has_schema_drift = EXCLUDED.has_schema_drift,
                    quality_status = EXCLUDED.quality_status,
                    updated_at = NOW()
                """
            )
        conn.commit()

    return metrics


def _prepare_batch_dataframe(
    batch_df: pl.DataFrame,
    schema_drift_columns: list[str],
) -> tuple[pl.DataFrame, dict[str, int]]:
    has_schema_drift = bool(schema_drift_columns)
    deduped_df = batch_df.unique(
        subset=BUSINESS_KEY_COLUMNS,
        keep="first",
        maintain_order=True,
    )
    duplicate_rows = batch_df.height - deduped_df.height

    prepared_df = deduped_df.with_columns(
        [
            _concat_key(BUSINESS_KEY_COLUMNS).alias("business_trip_key"),
            _concat_key(FINGERPRINT_COLUMNS).alias("row_fingerprint"),
            (
                (pl.col("fare_amount").fill_null(0.0) < 0.0)
                | (pl.col("total_amount").fill_null(0.0) < 0.0)
            ).alias("has_negative_fare"),
            (
                (pl.col("trip_distance").fill_null(0.0) < 0.0)
                | (pl.col("trip_distance").fill_null(0.0) > 250.0)
            ).alias("has_invalid_distance"),
            (
                pl.col("pickup_datetime").is_not_null()
                & pl.col("dropoff_datetime").is_not_null()
                & (pl.col("pickup_datetime") > pl.col("dropoff_datetime"))
            ).alias("has_temporal_anomaly"),
            (
                pl.col("pickup_location_id").is_null()
                | pl.col("dropoff_location_id").is_null()
                | (pl.col("pickup_location_id").fill_null(0) <= 0)
                | (pl.col("dropoff_location_id").fill_null(0) <= 0)
                | (pl.col("pickup_location_id").fill_null(0) > 265)
                | (pl.col("dropoff_location_id").fill_null(0) > 265)
            ).alias("has_invalid_zone"),
            (
                pl.col("vendor_id").is_null()
                | pl.col("pickup_datetime").is_null()
                | pl.col("dropoff_datetime").is_null()
            ).alias("has_null_key"),
            pl.lit(has_schema_drift).alias("has_schema_drift"),
        ]
    ).with_columns(
        [
            pl.when(
                pl.col("has_negative_fare")
                | pl.col("has_invalid_distance")
                | pl.col("has_temporal_anomaly")
                | pl.col("has_invalid_zone")
                | pl.col("has_null_key")
                | pl.col("has_schema_drift")
            )
            .then(pl.lit("flagged"))
            .otherwise(pl.lit("valid"))
            .alias("quality_status")
        ]
    )

    metrics = {
        "loaded_rows": int(prepared_df.height),
        "duplicate_rows": int(duplicate_rows),
        "invalid_rows": int(prepared_df.filter(pl.col("quality_status") == "flagged").height),
        "null_key_rows": int(prepared_df.filter(pl.col("has_null_key")).height),
    }
    return prepared_df, metrics


def _concat_key(columns: list[str]) -> pl.Expr:
    return pl.concat_str(
        [
            pl.col(column).cast(pl.Utf8, strict=False).fill_null("")
            for column in columns
        ],
        separator="|",
        ignore_nulls=False,
    )


def _copy_rows(
    cur: psycopg.Cursor[Any],
    table_name: str,
    columns: list[str],
    rows: Iterable[tuple[Any, ...]],
) -> None:
    column_list = ", ".join(columns)
    with cur.copy(f"COPY {table_name} ({column_list}) FROM STDIN") as copy:
        for row in rows:
            copy.write_row(tuple(_normalize_scalar(value) for value in row))


def _normalize_scalar(value: Any) -> Any:
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, datetime) and value.tzinfo is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value
