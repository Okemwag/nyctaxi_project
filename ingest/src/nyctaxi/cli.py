from __future__ import annotations

import argparse
import json

from nyctaxi.config import Settings
from nyctaxi.pipeline import bootstrap, run_month_pipeline, sync_zone_lookup
from nyctaxi.warehouse import ensure_warehouse


def main() -> None:
    parser = argparse.ArgumentParser(description="NYC Taxi lakehouse pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("bootstrap", help="Create schemas and sync the zone lookup")
    subparsers.add_parser("ensure-warehouse", help="Create schemas and base tables")
    subparsers.add_parser("load-zones", help="Refresh the taxi zone lookup")

    run_month = subparsers.add_parser("run-month", help="Ingest and load one taxi month")
    run_month.add_argument("--taxi-type", default="yellow", choices=["yellow", "green"])
    run_month.add_argument("--year-month", required=True, help="Month to ingest, e.g. 2024-01")

    args = parser.parse_args()
    settings = Settings.from_env()

    if args.command == "bootstrap":
        bootstrap(settings)
        print(json.dumps({"status": "success", "action": "bootstrap"}))
        return

    if args.command == "ensure-warehouse":
        ensure_warehouse(settings.warehouse_dsn)
        print(json.dumps({"status": "success", "action": "ensure-warehouse"}))
        return

    if args.command == "load-zones":
        count = sync_zone_lookup(settings)
        print(json.dumps({"status": "success", "action": "load-zones", "rows": count}))
        return

    result = run_month_pipeline(settings, taxi_type=args.taxi_type, year_month=args.year_month)
    print(json.dumps(result, default=str))


if __name__ == "__main__":
    main()

