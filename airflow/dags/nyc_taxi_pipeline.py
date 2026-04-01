from __future__ import annotations

import subprocess

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context
from pendulum import datetime

from nyctaxi.config import Settings
from nyctaxi.pipeline import run_month_pipeline, sync_zone_lookup
from nyctaxi.warehouse import ensure_warehouse


@dag(
    dag_id="nyc_taxi_lakehouse",
    start_date=datetime(2024, 1, 2, tz="UTC"),
    schedule="0 6 2 * *",
    catchup=False,
    params={
        "year_month": Param("", type=["null", "string"]),
        "taxi_type": Param("yellow", enum=["yellow", "green"]),
    },
    tags=["lakehouse", "nyc-taxi"],
)
def nyc_taxi_lakehouse():
    @task
    def resolve_params() -> dict[str, str]:
        context = get_current_context()
        configured_month = context["params"].get("year_month")
        year_month = configured_month or context["data_interval_start"].strftime("%Y-%m")
        return {
            "year_month": year_month,
            "taxi_type": context["params"].get("taxi_type", "yellow"),
        }

    @task
    def bootstrap() -> None:
        settings = Settings.from_env()
        ensure_warehouse(settings.warehouse_dsn)
        sync_zone_lookup(settings)

    @task
    def ingest_month(payload: dict[str, str]) -> dict[str, object]:
        settings = Settings.from_env()
        return run_month_pipeline(
            settings,
            taxi_type=payload["taxi_type"],
            year_month=payload["year_month"],
        )

    @task
    def build_dbt() -> None:
        commands = [
            [
                "dbt",
                "run",
                "--project-dir",
                "/opt/project/dbt",
                "--profiles-dir",
                "/opt/project/dbt",
            ],
            [
                "dbt",
                "test",
                "--project-dir",
                "/opt/project/dbt",
                "--profiles-dir",
                "/opt/project/dbt",
            ],
        ]
        for command in commands:
            subprocess.run(command, check=True)

    params = resolve_params()
    boot = bootstrap()
    loaded = ingest_month(params)
    built = build_dbt()

    boot >> loaded >> built


nyc_taxi_lakehouse()

