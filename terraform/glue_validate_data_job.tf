# --- AWS Glue Job for validating processed data ---

# Upload glue_validate_data.py script to S3
# resource "aws_s3_object" "glue_validate_data_script" {
#   bucket  = aws_s3_bucket.data_bucket.id
#   key     = "scripts/glue_validate_data.py"
#   content = file("${path.module}/helper/glue_validate_data.py")
#   etag    = filemd5("${path.module}/helper/glue_validate_data.py")
# }

# Create glue_validate_data Job
resource "aws_glue_job" "glue_validate_data" {
  name              = "glue-validate-data-${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "3.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  # Command configuration
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_bucket.bucket}/scripts/glue_validate_data.py"
    python_version  = "3"
  }

  # Default arguments required by the validation script
  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_bucket.bucket}/temp/"
    "--job-bookmark-option"              = "job-bookmark-disable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_bucket.bucket}/spark-logs/"
    "--data_bucket_name"                 = aws_s3_bucket.data_bucket.bucket
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
    Name = "glue-validate-data-${var.environment}"
  }

  # Depends on the script being uploaded to S3 first
  # depends_on = [aws_s3_object.glue_validate_data_script]
}

# CloudWatch Log Group for the Glue Validation Job
resource "aws_cloudwatch_log_group" "glue_validate_data_job_logs" {
  name              = "/aws-glue/jobs/glue-validate-data-${var.environment}"
  retention_in_days = 14
}