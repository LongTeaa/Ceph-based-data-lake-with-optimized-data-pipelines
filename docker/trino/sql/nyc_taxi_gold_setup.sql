CREATE SCHEMA IF NOT EXISTS lake.nyc_taxi
WITH (location = 's3://datalake-system/trino/nyc_taxi');

CREATE TABLE IF NOT EXISTS lake.nyc_taxi.daily_trip_metrics (
  trip_count BIGINT,
  total_revenue DOUBLE,
  fare_revenue DOUBLE,
  total_tip DOUBLE,
  avg_trip_distance DOUBLE,
  avg_passenger_count DOUBLE,
  pickup_date DATE
)
WITH (
  external_location = 's3://datalake-gold/daily_trip_metrics/year=2025/month=01',
  format = 'PARQUET',
  partitioned_by = ARRAY['pickup_date']
);

CREATE TABLE IF NOT EXISTS lake.nyc_taxi.location_metrics (
  pu_location_id BIGINT,
  do_location_id BIGINT,
  trip_count BIGINT,
  total_revenue DOUBLE,
  avg_trip_distance DOUBLE,
  pickup_date DATE
)
WITH (
  external_location = 's3://datalake-gold/location_metrics/year=2025/month=01',
  format = 'PARQUET',
  partitioned_by = ARRAY['pickup_date']
);

CREATE TABLE IF NOT EXISTS lake.nyc_taxi.payment_metrics (
  payment_type BIGINT,
  trip_count BIGINT,
  total_revenue DOUBLE,
  fare_revenue DOUBLE,
  total_tip DOUBLE,
  avg_tip_amount DOUBLE,
  pickup_date DATE
)
WITH (
  external_location = 's3://datalake-gold/payment_metrics/year=2025/month=01',
  format = 'PARQUET',
  partitioned_by = ARRAY['pickup_date']
);

CALL lake.system.sync_partition_metadata('nyc_taxi', 'daily_trip_metrics', 'FULL');
CALL lake.system.sync_partition_metadata('nyc_taxi', 'location_metrics', 'FULL');
CALL lake.system.sync_partition_metadata('nyc_taxi', 'payment_metrics', 'FULL');
