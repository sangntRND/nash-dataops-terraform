# AWS Glue Job for processing raw data

# Upload glue process raw data job script to S3 at the first time for easier demo purpose and then it was managed by another repository
# resource "aws_s3_object" "glue_process_raw_data_script" {
#   bucket  = aws_s3_bucket.data_bucket.id
#   key     = "scripts/glue_process_raw_data.py"
#   content = file("${path.module}/helper/glue_process_raw_data.py")
#   etag    = filemd5("${path.module}/helper/glue_process_raw_data.py")
# }

# Create glue process raw data Job
resource "aws_glue_job" "glue_process_raw_data" {
  name              = "glue-process-raw-data-${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "3.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  # Command configuration
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_bucket.bucket}/scripts/glue_process_raw_data.py"
    python_version  = "3"
  }

  # Default arguments
  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_bucket.bucket}/temp/"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_bucket.bucket}/spark-logs/"
    "--data_bucket_name"                 = aws_s3_bucket.data_bucket.bucket
    "--database_name"                    = aws_glue_catalog_database.demo_db.name
    "--fhvhv_table_name"                 = "raw_fhvhv_trips" # The table created by the crawler with raw_ prefix
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
  }

  # Execution properties
  execution_property {
    max_concurrent_runs = 1
  }

  # Tags
  tags = {
    Name = "glue-process-raw-data-${var.environment}"
  }

  # Depends on the script being uploaded to S3
  # depends_on = [aws_s3_object.glue_process_raw_data_script]
}

# CloudWatch Log Group for Glue Process Raw Data Job
resource "aws_cloudwatch_log_group" "glue_process_raw_data_job_logs" {
  name              = "/aws-glue/jobs/glue-process-raw-data-${var.environment}"
  retention_in_days = 14
}