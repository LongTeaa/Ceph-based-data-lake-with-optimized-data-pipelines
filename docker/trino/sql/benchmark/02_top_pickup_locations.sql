SELECT
  pickup_date,
  pu_location_id,
  SUM(trip_count) AS trip_count,
  SUM(total_revenue) AS total_revenue
FROM location_metrics
WHERE pickup_date BETWEEN DATE '2025-01-01' AND DATE '2025-01-31'
GROUP BY pickup_date, pu_location_id
ORDER BY trip_count DESC, pickup_date, pu_location_id
LIMIT 10
