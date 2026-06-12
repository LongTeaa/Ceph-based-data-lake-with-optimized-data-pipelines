SELECT
  payment_type,
  SUM(trip_count) AS trip_count,
  SUM(total_tip) / SUM(trip_count) AS avg_tip_per_trip,
  SUM(total_revenue) AS total_revenue
FROM payment_metrics
GROUP BY payment_type
ORDER BY payment_type
