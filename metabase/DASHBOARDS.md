# Dashboard Spec

Use this as the default Metabase collection layout for the project.

## 1. Executive Overview

Primary cards:

- `Monthly Trip Volume and Revenue`
- `Average Fare and Distance`
- `Flagged Trip Count`
- `Freshness Lag Hours`

Recommended filters:

- `taxi_type`
- `trip_month`

## 2. Operations Monitoring

Primary cards:

- `Pipeline Monitoring`
- `Active Alerts`
- `Source vs Warehouse Reconciliation`

Recommended filters:

- `taxi_type`
- `year_month`
- `severity`

## 3. Fare Anomalies

Primary cards:

- `Fare Anomaly Review`
- `Negative Fare Count`
- `Invalid Distance Count`
- `Temporal Anomaly Count`

Recommended filters:

- `taxi_type`
- `anomaly_type`
- `pickup_datetime`

## 4. Peak Pickup Zones

Primary cards:

- `Peak Pickup Zones by Hour`
- `Revenue by Pickup Borough`

Recommended filters:

- `taxi_type`
- `pickup_date`
- `pickup_hour`
- `pickup_borough`

## Visual Guidance

- Use line charts for monthly trend cards.
- Use tables with conditional formatting for reconciliation and active alerts.
- Use horizontal bar charts for top zones.
- Add a dashboard-level `taxi_type` filter to every dashboard.
- Pin `Active Alerts` and `Freshness Lag Hours` to the top of the operations dashboard.

