# Create Glue Database 
# Glue Database is used to store metadata tables
resource "aws_glue_catalog_database" "demo_db" {
  name        = "etl_demo_db_${var.environment}"
  description = "Database for ETL demo in ${var.environment} environment"
}

# Create Glue Crawler
# Glue Crawler is used to scan the raw data and processed data in S3 and create metadata tables in the Glue Data Catalog

# We will use 2 crawlers to scan raw and processed data in which:
# 1. Raw data: is kind of data that downloaded from https://www.nyc.gov/site/tlc/about/fhv-trip-record-data.page and put to raw folder in s3 (data lake)
# 2. Processed data: is kind of data that processed from raw data and put to processed folder in s3 (data lake)

# Crawler for raw data sources
resource "aws_glue_crawler" "raw_data_crawler" {
  name          = "raw-data-crawler-${var.environment}"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.demo_db.name
  table_prefix  = "raw_"

  s3_target {
    path       = "s3://${aws_s3_bucket.data_bucket.bucket}/raw/fhvhv_trips/"
    exclusions = ["*.csv"] # Exclude non-parquet files like taxi_zone_lookup.csv
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = {
    Name = "raw-data-crawler-${var.environment}"
  }
}

# Crawler for processed data
resource "aws_glue_crawler" "processed_data_crawler" {
  name          = "processed-data-crawler-${var.environment}"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.demo_db.name
  table_prefix  = "processed_"

  s3_target {
    path = "s3://${aws_s3_bucket.data_bucket.bucket}/processed/fhvhv_trips/"
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "UPDATE_IN_DATABASE"
  }

  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = {
    Name = "processed-data-crawler-${var.environment}"
  }
}
