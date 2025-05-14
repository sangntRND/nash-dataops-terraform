# AWS Glue Job configuration for loading data from S3 to Redshift

# Get the subnet's availability zone
data "aws_subnet" "selected" {
  id = data.aws_subnets.default.ids[0]
}

# Create Glue connection to Redshift
resource "aws_glue_connection" "redshift_connection" {
  name        = "redshift-connection-${var.environment}"
  description = "Connection to Redshift cluster for data loading"

  connection_properties = {
    JDBC_CONNECTION_URL = "jdbc:redshift://${aws_redshift_cluster.dataops_redshift.endpoint}/${var.redshift_database}"
    USERNAME            = var.redshift_username
    PASSWORD            = var.redshift_password
  }

  physical_connection_requirements {
    availability_zone      = data.aws_subnet.selected.availability_zone
    security_group_id_list = [aws_security_group.redshift_security_group.id]
    subnet_id              = data.aws_subnet.selected.id
  }

  depends_on = [aws_redshift_cluster.dataops_redshift]
}

# Upload schema from catalog job script to S3
resource "aws_s3_object" "schema_from_catalog_job_script" {
  bucket  = aws_s3_bucket.data_bucket.id
  key     = "scripts/redshift_schema_from_catalog_job.py"
  content = file("${path.module}/helper/redshift_schema_from_catalog_job.py")
  etag    = filemd5("${path.module}/helper/redshift_schema_from_catalog_job.py")
}

# Upload Redshift load job script to S3
resource "aws_s3_object" "redshift_load_job_script" {
  bucket  = aws_s3_bucket.data_bucket.id
  key     = "scripts/fhvhv_redshift_load_job.py"
  content = file("${path.module}/helper/fhvhv_redshift_load_job.py")
  etag    = filemd5("${path.module}/helper/fhvhv_redshift_load_job.py")
}

# Create schema from catalog Glue Job
resource "aws_glue_job" "schema_from_catalog_job" {
  name              = "fhvhv-schema-from-catalog-job-${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "3.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_bucket.bucket}/scripts/redshift_schema_from_catalog_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_bucket.bucket}/temp/"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_bucket.bucket}/spark-logs/"
    "--database_name"                    = aws_glue_catalog_database.demo_db.name
    "--table_name"                       = "processed_fhvhv_trips"
    "--redshift_connection"              = aws_glue_connection.redshift_connection.name
    "--redshift_database"                = var.redshift_database
    "--redshift_schema"                  = var.redshift_schema
    "--redshift_table"                   = var.redshift_table
    "--redshift_host"                    = aws_redshift_cluster.dataops_redshift.endpoint
    "--redshift_username"                = var.redshift_username
    "--redshift_password"                = var.redshift_password
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
    "--additional-python-modules"         = "psycopg2-binary==2.9.9"
    "--conf"                             = "spark.sql.legacy.timeParserPolicy=LEGACY --conf spark.sql.legacy.parquet.int96RebaseModeInRead=LEGACY --conf spark.sql.legacy.parquet.int96RebaseModeInWrite=LEGACY"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  # connections = [aws_glue_connection.redshift_connection.name]

  tags = {
    Name = "fhvhv-schema-from-catalog-job-${var.environment}"
  }

  depends_on = [aws_s3_object.schema_from_catalog_job_script]
}

# Create Redshift load Glue Job
resource "aws_glue_job" "redshift_load_job" {
  name              = "fhvhv-redshift-load-job-${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  glue_version      = "3.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 60

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.data_bucket.bucket}/scripts/fhvhv_redshift_load_job.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                     = "python"
    "--TempDir"                          = "s3://${aws_s3_bucket.data_bucket.bucket}/temp/"
    "--job-bookmark-option"              = "job-bookmark-enable"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"                  = "true"
    "--spark-event-logs-path"            = "s3://${aws_s3_bucket.data_bucket.bucket}/spark-logs/"
    "--data_bucket_name"                 = aws_s3_bucket.data_bucket.bucket
    "--database_name"                    = aws_glue_catalog_database.demo_db.name
    "--redshift_host"                    = aws_redshift_cluster.dataops_redshift.endpoint
    "--redshift_username"                = var.redshift_username
    "--redshift_password"                = var.redshift_password
    "--redshift_connection"              = aws_glue_connection.redshift_connection.name
    "--redshift_database"                = var.redshift_database
    "--redshift_schema"                  = var.redshift_schema
    "--redshift_table"                   = var.redshift_table
    "--enable-job-insights"              = "true"
    "--enable-glue-datacatalog"          = "true"
    "--enable-metrics"                   = "true"
  }

  execution_property {
    max_concurrent_runs = 1
  }

  # connections = [aws_glue_connection.redshift_connection.name]

  tags = {
    Name = "fhvhv-redshift-load-job-${var.environment}"
  }

  depends_on = [aws_s3_object.redshift_load_job_script]
}

# Create workflow to coordinate the ETL jobs
resource "aws_glue_workflow" "fhvhv_etl_workflow" {
  name        = "fhvhv-etl-workflow-${var.environment}"
  description = "Workflow to coordinate FHVHV ETL and Redshift load jobs"
}

# Trigger to run schema creation job after processed data crawler completes
resource "aws_glue_trigger" "schema_creation_after_crawler" {
  name          = "schema-creation-after-crawler-${var.environment}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.fhvhv_etl_workflow.name

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.processed_fhvhv_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.schema_from_catalog_job.name
  }
}

# Modify trigger to start load job after schema creation
resource "aws_glue_trigger" "redshift_load_trigger" {
  name          = "redshift-load-trigger-${var.environment}"
  workflow_name = aws_glue_workflow.fhvhv_etl_workflow.name
  type          = "CONDITIONAL"

  actions {
    job_name = aws_glue_job.redshift_load_job.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.schema_from_catalog_job.name
      state    = "SUCCEEDED"
    }
  }
}