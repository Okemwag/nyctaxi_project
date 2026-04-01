from __future__ import annotations

import polars as pl

from nyctaxi.pipeline import month_range, normalize_trip_file


def test_normalize_trip_file_maps_aliases_and_tracks_schema_drift(tmp_path) -> None:
    frame = pl.DataFrame(
        {
            "VendorID": [1],
            "tpep_pickup_datetime": ["2024-01-01 10:00:00"],
            "tpep_dropoff_datetime": ["2024-01-01 10:15:00"],
            "passenger_count": [2],
            "trip_distance": [4.2],
            "PULocationID": [100],
            "DOLocationID": [161],
            "fare_amount": [18.5],
            "total_amount": [24.0],
            "bonus_field": ["drift"],
        }
    )
    path = tmp_path / "yellow_tripdata_2024-01.parquet"
    frame.write_parquet(path)

    normalized = normalize_trip_file(
        file_path=str(path),
        taxi_type="yellow",
        year_month="2024-01",
        batch_id="batch-1",
    )

    assert normalized.source_rows == 1
    assert normalized.schema_drift_columns == ["bonus_field"]
    assert normalized.dataframe["vendor_id"].to_list() == [1]
    assert normalized.dataframe["pickup_location_id"].to_list() == [100]
    assert normalized.dataframe["source_year_month"].to_list() == ["2024-01"]


def test_month_range_is_inclusive() -> None:
    assert month_range("2024-01", "2024-03") == ["2024-01", "2024-02", "2024-03"]
