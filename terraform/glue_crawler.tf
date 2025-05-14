# Glue Crawler configuration
# This crawler will scan the raw data in S3 and create metadata tables in the Glue Data Catalog

# Create IAM role for Glue Crawler
resource "aws_iam_role" "glue_crawler_role" {
  name = "glue-crawler-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
}

# Attach policies to Glue Crawler role
resource "aws_iam_role_policy_attachment" "glue_crawler_service" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_crawler_s3_access" {
  name = "glue-crawler-s3-access-policy"
  role = aws_iam_role.glue_crawler_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.data_bucket.arn,
          "${aws_s3_bucket.data_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Glue Crawler configuration for raw and processed data

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

# Crawler for processed FHVHV data
resource "aws_glue_crawler" "processed_fhvhv_crawler" {
  name          = "processed-fhvhv-crawler-${var.environment}"
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
    Name = "processed-fhvhv-crawler-${var.environment}"
  }
}

# Schedule for raw data crawler
resource "aws_glue_trigger" "raw_data_crawler_schedule" {
  name     = "raw-data-crawler-schedule-${var.environment}"
  type     = "SCHEDULED"
  schedule = "cron(0 0 * * ? *)" # Run daily at midnight UTC
  workflow_name = aws_glue_workflow.fhvhv_etl_workflow.name

  actions {
    crawler_name = aws_glue_crawler.raw_data_crawler.name
  }
}

# Trigger to start ETL job after raw data crawler completes
resource "aws_glue_trigger" "start_etl_after_crawler" {
  name          = "start-etl-after-crawler-${var.environment}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.fhvhv_etl_workflow.name

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.raw_data_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.fhvhv_etl_job.name
  }
}

# Trigger to run processed FHVHV crawler after ETL job completes
resource "aws_glue_trigger" "fhvhv_etl_job_completion" {
  name = "fhvhv-etl-job-completion-${var.environment}"
  type = "CONDITIONAL"
  workflow_name = aws_glue_workflow.fhvhv_etl_workflow.name

  predicate {
    conditions {
      job_name = aws_glue_job.fhvhv_etl_job.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = aws_glue_crawler.processed_fhvhv_crawler.name
  }
}
