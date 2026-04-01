SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec

SELECT 'CREATE DATABASE nyctaxi'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'nyctaxi')\gexec

SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'warehouse') THEN
        CREATE ROLE warehouse LOGIN PASSWORD 'warehouse';
    END IF;
END
$$;

GRANT ALL PRIVILEGES ON DATABASE nyctaxi TO warehouse;

