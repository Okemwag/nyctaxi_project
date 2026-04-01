from __future__ import annotations

EXPECTED_TRIP_COLUMNS = [
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

TRIP_COLUMN_ALIASES = {
    "vendorid": "vendor_id",
    "vendor_id": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "ratecodeid": "rate_code_id",
    "rate_code_id": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "pulocationid": "pickup_location_id",
    "pickup_location_id": "pickup_location_id",
    "dolocationid": "dropoff_location_id",
    "dropoff_location_id": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "airport_fee",
    "trip_type": "trip_type",
    "ehail_fee": "ehail_fee",
}

INTEGER_COLUMNS = {
    "vendor_id",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "rate_code_id",
    "trip_type",
}

FLOAT_COLUMNS = {
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
    "airport_fee",
    "ehail_fee",
}

STRING_COLUMNS = {"store_and_fwd_flag"}

DATETIME_COLUMNS = {"pickup_datetime", "dropoff_datetime"}

BUSINESS_KEY_COLUMNS = [
    "taxi_type",
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "pickup_location_id",
    "dropoff_location_id",
    "passenger_count",
]

FINGERPRINT_COLUMNS = BUSINESS_KEY_COLUMNS + [
    "trip_distance",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]

