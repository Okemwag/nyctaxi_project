from __future__ import annotations

import os
from dataclasses import dataclass


def _as_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    warehouse_dsn: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    raw_bucket: str
    source_base_url: str
    zone_lookup_url: str
    minio_secure: bool = False

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            warehouse_dsn=os.getenv(
                "WAREHOUSE_DSN",
                "postgresql://warehouse:warehouse@localhost:5432/nyctaxi",
            ),
            minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
            minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minio"),
            minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minio123"),
            raw_bucket=os.getenv("RAW_BUCKET", "nyctaxi-lake"),
            source_base_url=os.getenv(
                "SOURCE_BASE_URL",
                "https://d37ci6vzurychx.cloudfront.net/trip-data",
            ),
            zone_lookup_url=os.getenv(
                "ZONE_LOOKUP_URL",
                "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
            ),
            minio_secure=_as_bool(os.getenv("MINIO_SECURE"), default=False),
        )

