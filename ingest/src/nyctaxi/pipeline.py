from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from email.utils import parsedate_to_datetime
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import boto3
import polars as pl
import requests

from nyctaxi.config import Settings
from nyctaxi.models import (
    DATETIME_COLUMNS,
    EXPECTED_TRIP_COLUMNS,
    FLOAT_COLUMNS,
    INTEGER_COLUMNS,
    STRING_COLUMNS,
    TRIP_COLUMN_ALIASES,
)
from nyctaxi.warehouse import (
    batch_already_succeeded,
    ensure_warehouse,
    fail_batch,
    finish_batch,
    load_trip_batch,
    start_batch,
    upsert_zone_lookup,
)


@dataclass(frozen=True)
class NormalizedBatch:
    dataframe: pl.DataFrame
    source_rows: int
    schema_drift_columns: list[str]
    source_file_name: str


def month_range(start_month: str, end_month: str) -> list[str]:
    start = _parse_year_month(start_month)
    end = _parse_year_month(end_month)
    if start > end:
        raise ValueError("start_month must be less than or equal to end_month")

    months: list[str] = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        year = current.year + (1 if current.month == 12 else 0)
        month = 1 if current.month == 12 else current.month + 1
        current = current.replace(year=year, month=month)
    return months


def run_month_range(
    settings: Settings,
    taxi_type: str,
    start_month: str,
    end_month: str,
    continue_on_error: bool = False,
) -> dict[str, object]:
    results: list[dict[str, object]] = []
    failures: list[dict[str, str]] = []

    for year_month in month_range(start_month, end_month):
        try:
            results.append(
                run_month_pipeline(
                    settings=settings,
                    taxi_type=taxi_type,
                    year_month=year_month,
                )
            )
        except Exception as exc:
            failure = {"year_month": year_month, "error": str(exc)}
            failures.append(failure)
            if not continue_on_error:
                raise

    return {
        "status": "partial_success" if failures else "success",
        "taxi_type": taxi_type,
        "start_month": start_month,
        "end_month": end_month,
        "requested_months": month_range(start_month, end_month),
        "successful_months": [
            result["year_month"]
            for result in results
            if result.get("status") in {"success", "skipped"}
        ],
        "failed_months": failures,
        "results": results,
    }


def bootstrap(settings: Settings) -> None:
    ensure_warehouse(settings.warehouse_dsn)
    sync_zone_lookup(settings)


def sync_zone_lookup(settings: Settings) -> int:
    ensure_warehouse(settings.warehouse_dsn)
    response = requests.get(settings.zone_lookup_url, timeout=120)
    response.raise_for_status()
    zones = pl.read_csv(BytesIO(response.content))
    zones = zones.rename(
        {
            "LocationID": "location_id",
            "Borough": "borough",
            "Zone": "zone",
            "service_zone": "service_zone",
        }
    ).select(["location_id", "borough", "zone", "service_zone"])
    zones = zones.with_columns(
        [
            pl.col("location_id").cast(pl.Int64, strict=False),
            pl.col("borough").cast(pl.Utf8, strict=False),
            pl.col("zone").cast(pl.Utf8, strict=False),
            pl.col("service_zone").cast(pl.Utf8, strict=False),
        ]
    )
    return upsert_zone_lookup(settings.warehouse_dsn, zones)


def run_month_pipeline(settings: Settings, taxi_type: str, year_month: str) -> dict[str, object]:
    ensure_warehouse(settings.warehouse_dsn)
    batch_id = str(uuid4())
    source_url = build_source_url(settings.source_base_url, taxi_type, year_month)
    source_file_name = Path(source_url).name
    local_path, headers = download_to_temp(source_url)
    source_etag = headers.get("ETag")
    source_last_modified = _parse_http_datetime(headers.get("Last-Modified"))

    try:
        if batch_already_succeeded(
            settings.warehouse_dsn,
            taxi_type=taxi_type,
            year_month=year_month,
            source_etag=source_etag,
        ):
            return {
                "status": "skipped",
                "reason": "batch with the same source etag already succeeded",
                "taxi_type": taxi_type,
                "year_month": year_month,
                "source_url": source_url,
                "source_etag": source_etag,
            }

        normalized = normalize_trip_file(
            file_path=local_path,
            taxi_type=taxi_type,
            year_month=year_month,
            batch_id=batch_id,
            source_file_name=source_file_name,
        )
        bronze_object_key = (
            f"bronze/{taxi_type}/year_month={year_month}/{normalized.source_file_name}"
        )
        upload_raw_file(settings, local_path, bronze_object_key)

        start_batch(
            dsn=settings.warehouse_dsn,
            batch_id=batch_id,
            taxi_type=taxi_type,
            year_month=year_month,
            source_url=source_url,
            source_file_name=normalized.source_file_name,
            bronze_object_key=bronze_object_key,
            source_etag=source_etag,
            source_last_modified=source_last_modified,
            source_rows=normalized.source_rows,
            schema_drift_columns=normalized.schema_drift_columns,
        )
        metrics = load_trip_batch(
            dsn=settings.warehouse_dsn,
            batch_df=normalized.dataframe,
            schema_drift_columns=normalized.schema_drift_columns,
        )
        finish_batch(settings.warehouse_dsn, batch_id=batch_id, **metrics)

        return {
            "status": "success",
            "batch_id": batch_id,
            "taxi_type": taxi_type,
            "year_month": year_month,
            "source_url": source_url,
            "source_rows": normalized.source_rows,
            "schema_drift_columns": normalized.schema_drift_columns,
            **metrics,
        }
    except Exception as exc:
        fail_batch(settings.warehouse_dsn, batch_id=batch_id, error_message=str(exc))
        raise
    finally:
        os.unlink(local_path)


def build_source_url(source_base_url: str, taxi_type: str, year_month: str) -> str:
    return f"{source_base_url.rstrip('/')}/{taxi_type}_tripdata_{year_month}.parquet"


def download_to_temp(source_url: str) -> tuple[str, dict[str, str]]:
    response = requests.get(source_url, stream=True, timeout=300)
    response.raise_for_status()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".parquet") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
        return handle.name, dict(response.headers)


def upload_raw_file(settings: Settings, local_path: str, object_key: str) -> None:
    client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_access_key,
        aws_secret_access_key=settings.minio_secret_key,
        region_name="us-east-1",
    )

    try:
        client.head_bucket(Bucket=settings.raw_bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=settings.raw_bucket)

    client.upload_file(local_path, settings.raw_bucket, object_key)


def normalize_trip_file(
    file_path: str,
    taxi_type: str,
    year_month: str,
    batch_id: str,
    source_file_name: str | None = None,
) -> NormalizedBatch:
    raw_df = pl.read_parquet(file_path)
    source_rows = raw_df.height
    original_columns = list(raw_df.columns)
    rename_map = {
        column: TRIP_COLUMN_ALIASES[column.lower()]
        for column in original_columns
        if column.lower() in TRIP_COLUMN_ALIASES
    }
    schema_drift_columns = sorted(
        column for column in original_columns if column.lower() not in TRIP_COLUMN_ALIASES
    )
    normalized = raw_df.rename(rename_map)

    for column in EXPECTED_TRIP_COLUMNS:
        if column not in normalized.columns:
            normalized = normalized.with_columns(pl.lit(None).alias(column))

    normalized = normalized.select(EXPECTED_TRIP_COLUMNS).with_row_index(
        name="row_number_in_file",
        offset=1,
    )
    normalized = normalized.with_columns(_cast_trip_columns())
    normalized = normalized.with_columns(
        [
            pl.lit(batch_id).alias("batch_id"),
            pl.lit(taxi_type).alias("taxi_type"),
            pl.lit(year_month).alias("source_year_month"),
            pl.lit(source_file_name or Path(file_path).name).alias("source_file_name"),
        ]
    )
    ordered_columns = [
        "batch_id",
        "taxi_type",
        "source_year_month",
        "source_file_name",
        "row_number_in_file",
        *EXPECTED_TRIP_COLUMNS,
    ]
    normalized = normalized.select(ordered_columns)
    return NormalizedBatch(
        dataframe=normalized,
        source_rows=source_rows,
        schema_drift_columns=schema_drift_columns,
        source_file_name=source_file_name or Path(file_path).name,
    )


def _cast_trip_columns() -> list[pl.Expr]:
    expressions: list[pl.Expr] = []
    for column in EXPECTED_TRIP_COLUMNS:
        if column in DATETIME_COLUMNS:
            expressions.append(
                pl.col(column)
                .cast(pl.Utf8, strict=False)
                .str.strptime(pl.Datetime, strict=False, exact=False)
                .alias(column)
            )
        elif column in INTEGER_COLUMNS:
            expressions.append(pl.col(column).cast(pl.Int64, strict=False).alias(column))
        elif column in FLOAT_COLUMNS:
            expressions.append(pl.col(column).cast(pl.Float64, strict=False).alias(column))
        elif column in STRING_COLUMNS:
            expressions.append(pl.col(column).cast(pl.Utf8, strict=False).alias(column))
        else:
            expressions.append(pl.col(column))
    return expressions


def _parse_http_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    return parsedate_to_datetime(value)


def _parse_year_month(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m")
    except ValueError as exc:
        raise ValueError(f"invalid year_month '{value}', expected YYYY-MM") from exc
