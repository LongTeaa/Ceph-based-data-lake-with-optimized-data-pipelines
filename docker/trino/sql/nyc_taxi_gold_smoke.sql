SELECT
  COUNT(*) AS daily_rows,
  SUM(trip_count) AS total_trips,
  ROUND(SUM(total_revenue), 2) AS total_revenue
FROM lake.nyc_taxi.daily_trip_metrics;

SELECT
  pickup_date,
  trip_count,
  ROUND(total_revenue, 2) AS total_revenue
FROM lake.nyc_taxi.daily_trip_metrics
ORDER BY pickup_date
LIMIT 10;

SELECT
  payment_type,
  SUM(trip_count) AS trip_count,
  ROUND(SUM(total_tip) / SUM(trip_count), 2) AS avg_tip_per_trip
FROM lake.nyc_taxi.payment_metrics
GROUP BY payment_type
ORDER BY payment_type;
