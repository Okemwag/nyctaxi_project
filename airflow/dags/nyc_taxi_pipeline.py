from __future__ import annotations

import os
import subprocess
from datetime import timedelta

import psycopg
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from pendulum import datetime

from nyctaxi.config import Settings
from nyctaxi.pipeline import month_range, run_month_pipeline, sync_zone_lookup
from nyctaxi.warehouse import ensure_warehouse


@dag(
    dag_id="nyc_taxi_lakehouse",
    start_date=datetime(2024, 1, 2, tz="UTC"),
    schedule="0 6 2 * *",
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=8),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    params={
        "year_month": Param("", type=["null", "string"]),
        "start_month": Param("", type=["null", "string"]),
        "end_month": Param("", type=["null", "string"]),
        "taxi_type": Param("yellow", enum=["yellow", "green"]),
    },
    tags=["lakehouse", "nyc-taxi"],
)
def nyc_taxi_lakehouse():
    @task
    def resolve_payloads() -> list[dict[str, str]]:
        context = get_current_context()
        params = context["params"]
        taxi_type = params.get("taxi_type", "yellow")
        configured_month = _empty_to_none(params.get("year_month"))
        start_month = _empty_to_none(params.get("start_month"))
        end_month = _empty_to_none(params.get("end_month"))

        if configured_month and (start_month or end_month):
            raise ValueError("Provide either year_month or a start_month/end_month range, not both")

        if start_month or end_month:
            if not start_month or not end_month:
                raise ValueError("Both start_month and end_month are required for backfills")
            months = month_range(start_month, end_month)
        else:
            months = [configured_month or context["data_interval_start"].strftime("%Y-%m")]

        return [{"year_month": year_month, "taxi_type": taxi_type} for year_month in months]

    @task(execution_timeout=timedelta(minutes=30))
    def bootstrap() -> None:
        settings = Settings.from_env()
        ensure_warehouse(settings.warehouse_dsn)
        sync_zone_lookup(settings)

    @task(execution_timeout=timedelta(hours=2))
    def ingest_month(payload: dict[str, str]) -> dict[str, object]:
        settings = Settings.from_env()
        return run_month_pipeline(
            settings,
            taxi_type=payload["taxi_type"],
            year_month=payload["year_month"],
        )

    @task(execution_timeout=timedelta(minutes=30))
    def dbt_source_freshness() -> None:
        _run_dbt_command(["source", "freshness"])

    @task(execution_timeout=timedelta(hours=2))
    def dbt_run() -> None:
        _run_dbt_command(["run"])

    @task(execution_timeout=timedelta(hours=1))
    def dbt_test() -> None:
        _run_dbt_command(["test"])

    @task(execution_timeout=timedelta(minutes=10))
    def check_alerts() -> None:
        settings = Settings.from_env()
        with psycopg.connect(settings.warehouse_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    select severity, taxi_type, coalesce(year_month, ''), alert_code, alert_context
                    from gold.ops_alerts
                    order by severity desc, taxi_type, year_month nulls first, alert_code
                    """
                )
                alerts = cur.fetchall()

        error_alerts = [row for row in alerts if row[0] == "error"]
        if error_alerts:
            formatted = "; ".join(
                f"{severity}:{taxi_type}:{year_month}:{alert_code}:{alert_context}"
                for severity, taxi_type, year_month, alert_code, alert_context in error_alerts
            )
            raise AirflowException(f"Operational alert threshold breached: {formatted}")

    payloads = resolve_payloads()
    boot = bootstrap()
    loaded = ingest_month.expand(payload=payloads)
    freshness = dbt_source_freshness()
    built = dbt_run()
    tested = dbt_test()
    alerted = check_alerts()

    boot >> loaded >> freshness >> built >> tested >> alerted


nyc_taxi_lakehouse()


def _empty_to_none(value: str | None) -> str | None:
    if not value:
        return None
    return value


def _run_dbt_command(arguments: list[str]) -> None:
    command = [
        "dbt",
        *arguments,
        "--project-dir",
        "/opt/project/dbt",
        "--profiles-dir",
        "/opt/project/dbt",
    ]
    env = {
        **os.environ,
        "DBT_TARGET_PATH": os.getenv("DBT_TARGET_PATH", "/tmp/dbt_target"),
        "DBT_LOG_PATH": os.getenv("DBT_LOG_PATH", "/tmp/dbt_logs"),
    }
    subprocess.run(command, check=True, env=env)
