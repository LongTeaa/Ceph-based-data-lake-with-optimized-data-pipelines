SELECT
  pickup_date,
  COUNT(*) AS trip_count,
  SUM(total_amount) AS total_revenue,
  AVG(trip_distance) AS avg_trip_distance
FROM silver_trips
WHERE pickup_date = DATE '${pickup_date}'
GROUP BY pickup_date
