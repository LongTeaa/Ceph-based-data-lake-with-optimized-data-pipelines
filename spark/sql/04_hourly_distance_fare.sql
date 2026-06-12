SELECT
  HOUR(pickup_datetime) AS pickup_hour,
  COUNT(*) AS trip_count,
  AVG(trip_distance) AS avg_trip_distance,
  AVG(fare_amount) AS avg_fare_amount,
  AVG(total_amount) AS avg_total_amount
FROM silver_trips
GROUP BY HOUR(pickup_datetime)
ORDER BY pickup_hour
