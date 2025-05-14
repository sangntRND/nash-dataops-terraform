-- Sample Athena queries for the DataOps ETL Demo
-- Replace 'etl_demo_db_dev' with your actual database name if different

-- Query 1: Basic SELECT from raw data
SELECT * 
FROM etl_demo_db_dev.raw_sample_data 
LIMIT 10;

-- Query 2: Count records in raw data
SELECT COUNT(*) AS record_count 
FROM etl_demo_db_dev.raw_sample_data;

-- Query 3: Basic SELECT from processed data
SELECT * 
FROM etl_demo_db_dev.processed_sample_data 
LIMIT 10;

-- Query 4: Count records in processed data
SELECT COUNT(*) AS record_count 
FROM etl_demo_db_dev.processed_sample_data;

-- Query 5: Data quality check - find null values
SELECT 
  SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id_count,
  SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS null_name_count,
  SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_value_count,
  SUM(CASE WHEN timestamp IS NULL THEN 1 ELSE 0 END) AS null_timestamp_count
FROM etl_demo_db_dev.processed_sample_data;

-- Query 6: Aggregation by month
SELECT 
  year,
  month,
  COUNT(*) AS record_count,
  AVG(value) AS avg_value,
  MIN(value) AS min_value,
  MAX(value) AS max_value
FROM etl_demo_db_dev.processed_sample_data
GROUP BY year, month
ORDER BY year, month;

-- Query 7: Time-based filtering
SELECT *
FROM etl_demo_db_dev.processed_sample_data
WHERE year = 2023 AND month = 1
LIMIT 10;

-- Query 8: Join raw and processed data
SELECT 
  r.id,
  r.name,
  r.value AS original_value,
  p.value AS processed_value,
  r.timestamp,
  p.processing_date
FROM etl_demo_db_dev.raw_sample_data r
JOIN etl_demo_db_dev.processed_sample_data p
  ON r.id = p.id
LIMIT 10;