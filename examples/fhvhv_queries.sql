-- Sample Athena queries for the FHVHV trip data
-- These queries demonstrate common analyses for FHV data

-- 1. Basic trip metrics by borough
SELECT 
  PU_Borough,
  COUNT(*) AS trip_count,
  ROUND(AVG(trip_miles), 2) AS avg_distance_miles,
  ROUND(AVG(CAST(DATE_DIFF('minute', pickup_datetime, dropoff_datetime) AS DOUBLE)), 2) AS avg_duration_minutes,
  COUNT(CASE WHEN shared_ride_status = 'Shared Ride' THEN 1 END) AS shared_ride_count,
  ROUND(COUNT(CASE WHEN shared_ride_status = 'Shared Ride' THEN 1 END) * 100.0 / COUNT(*), 2) AS shared_ride_percentage
FROM 
  etl_demo_db_dev.processed_fhvhv_trips
GROUP BY 
  PU_Borough
ORDER BY 
  trip_count DESC;

-- 2. Hourly trip patterns
SELECT 
  HOUR(pickup_datetime) AS hour_of_day,
  COUNT(*) AS trip_count,
  ROUND(AVG(trip_miles), 2) AS avg_distance
FROM 
  etl_demo_db_dev.processed_fhvhv_trips
GROUP BY 
  HOUR(pickup_datetime)
ORDER BY 
  hour_of_day;

-- 3. Popular pickup/dropoff zone pairs
SELECT 
  pt.PU_Zone AS pickup_zone,
  pt.DO_Zone AS dropoff_zone,
  COUNT(*) AS trip_count,
  ROUND(AVG(pt.trip_miles), 2) AS avg_distance,
  ROUND(AVG(CAST(DATE_DIFF('minute', pt.pickup_datetime, pt.dropoff_datetime) AS DOUBLE)), 2) AS avg_duration_minutes
FROM 
  etl_demo_db_dev.processed_fhvhv_trips pt
GROUP BY 
  pt.PU_Zone, pt.DO_Zone
HAVING 
  COUNT(*) > 100
ORDER BY 
  trip_count DESC
LIMIT 
  20;

-- 4. Shared ride analysis
SELECT 
  DATE(pickup_datetime) AS trip_date,
  COUNT(*) AS total_trips,
  COUNT(CASE WHEN shared_ride_status = 'Shared Ride' THEN 1 END) AS shared_rides,
  ROUND(COUNT(CASE WHEN shared_ride_status = 'Shared Ride' THEN 1 END) * 100.0 / COUNT(*), 2) AS shared_ride_percentage
FROM 
  etl_demo_db_dev.processed_fhvhv_trips
GROUP BY 
  DATE(pickup_datetime)
ORDER BY 
  trip_date;

-- 5. Base affiliation analysis
SELECT 
  originating_base_num,
  COUNT(*) AS trip_count,
  COUNT(DISTINCT dispatching_base_num) AS unique_dispatching_bases,
  ROUND(AVG(trip_miles), 2) AS avg_trip_distance,
  COUNT(CASE WHEN shared_ride_status = 'Shared Ride' THEN 1 END) AS shared_ride_count
FROM 
  etl_demo_db_dev.processed_fhvhv_trips
GROUP BY 
  originating_base_num
ORDER BY 
  trip_count DESC
LIMIT 
  20;

-- 6. Trip distance distribution
SELECT 
  CASE
    WHEN trip_miles BETWEEN 0 AND 1 THEN '0-1 miles'
    WHEN trip_miles BETWEEN 1 AND 2 THEN '1-2 miles'
    WHEN trip_miles BETWEEN 2 AND 3 THEN '2-3 miles'
    WHEN trip_miles BETWEEN 3 AND 5 THEN '3-5 miles'
    WHEN trip_miles BETWEEN 5 AND 10 THEN '5-10 miles'
    WHEN trip_miles > 10 THEN 'Over 10 miles'
  END AS distance_range,
  COUNT(*) AS trip_count,
  ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM 
  etl_demo_db_dev.processed_fhvhv_trips
GROUP BY 
  CASE
    WHEN trip_miles BETWEEN 0 AND 1 THEN '0-1 miles'
    WHEN trip_miles BETWEEN 1 AND 2 THEN '1-2 miles'
    WHEN trip_miles BETWEEN 2 AND 3 THEN '2-3 miles'
    WHEN trip_miles BETWEEN 3 AND 5 THEN '3-5 miles'
    WHEN trip_miles BETWEEN 5 AND 10 THEN '5-10 miles'
    WHEN trip_miles > 10 THEN 'Over 10 miles'
  END
ORDER BY 
  CASE
    WHEN distance_range = '0-1 miles' THEN 1
    WHEN distance_range = '1-2 miles' THEN 2
    WHEN distance_range = '2-3 miles' THEN 3
    WHEN distance_range = '3-5 miles' THEN 4
    WHEN distance_range = '5-10 miles' THEN 5
    WHEN distance_range = 'Over 10 miles' THEN 6
  END;

-- 7. Trip duration by service zone
SELECT 
  pt.PU_service_zone,
  COUNT(*) AS trip_count,
  ROUND(AVG(CAST(DATE_DIFF('minute', pt.pickup_datetime, pt.dropoff_datetime) AS DOUBLE)), 2) AS avg_duration_minutes,
  ROUND(MIN(CAST(DATE_DIFF('minute', pt.pickup_datetime, pt.dropoff_datetime) AS DOUBLE)), 2) AS min_duration,
  ROUND(MAX(CAST(DATE_DIFF('minute', pt.pickup_datetime, pt.dropoff_datetime) AS DOUBLE)), 2) AS max_duration
FROM 
  etl_demo_db_dev.processed_fhvhv_trips pt
GROUP BY 
  pt.PU_service_zone
ORDER BY 
  avg_duration_minutes DESC; 