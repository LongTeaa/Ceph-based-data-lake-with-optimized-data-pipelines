SELECT
  pu_location_id,
  do_location_id,
  COUNT(*) AS trip_count,
  SUM(total_amount) AS total_revenue,
  AVG(trip_distance) AS avg_trip_distance
FROM silver_trips
GROUP BY pu_location_id, do_location_id
ORDER BY trip_count DESC, pu_location_id, do_location_id
LIMIT 20
