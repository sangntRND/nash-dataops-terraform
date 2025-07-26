# Create workflow to coordinate the data pipeline
resource "aws_glue_workflow" "data_pipeline_workflow" {
  name        = "data-pipeline-workflow-${var.environment}"
  description = "Workflow to coordinate data pipeline jobs"
}

# 1. Schedule for raw data crawler
resource "aws_glue_trigger" "raw_data_crawler_schedule" {
  name     = "raw-data-crawler-schedule-${var.environment}"
  type     = "SCHEDULED"
  schedule = "cron(0 0 3 * ? *)" # Run monthly at 3rd day of month at midnight UTC
  workflow_name = aws_glue_workflow.data_pipeline_workflow.name

  actions {
    crawler_name = aws_glue_crawler.raw_data_crawler.name
  }
}

# 2. Trigger to start glue process raw data job after raw data crawler completes
resource "aws_glue_trigger" "glue_process_raw_data_trigger" {
  name          = "glue-process-raw-data-trigger-${var.environment}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.data_pipeline_workflow.name

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.raw_data_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.glue_process_raw_data.name
  }
}

# 3. Trigger to run validate data job after glue process raw data job completes
resource "aws_glue_trigger" "glue_validate_data_trigger" {
  name          = "glue-validate-data-trigger-${var.environment}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.data_pipeline_workflow.name

  predicate {
    conditions {
      job_name = aws_glue_job.glue_process_raw_data.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.glue_validate_data.name
  }
}

# 4. Trigger to run processed data crawler after glue process raw data job completes
resource "aws_glue_trigger" "processed_data_crawler_trigger" {
  name = "processed-data-crawler-trigger-${var.environment}"
  type = "CONDITIONAL"
  workflow_name = aws_glue_workflow.data_pipeline_workflow.name

  predicate {
    conditions {
      job_name = aws_glue_job.glue_validate_data.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    crawler_name = aws_glue_crawler.processed_data_crawler.name
  }
}

# 5. Trigger to run redshift schema creation job after processed data crawler completes
resource "aws_glue_trigger" "glue_manage_redshift_schema_job_trigger" {
  name          = "glue-manage-redshift-schema-job-trigger-${var.environment}"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.data_pipeline_workflow.name

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.processed_data_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.glue_manage_redshift_schema_job.name
  }
}

# 6. Trigger to run load job after redshift schema creation
resource "aws_glue_trigger" "glue_load_data_to_redshift_trigger" {
  name          = "glue-load-data-to-redshift-trigger-${var.environment}"
  workflow_name = aws_glue_workflow.data_pipeline_workflow.name
  type          = "CONDITIONAL"

  actions {
    job_name = aws_glue_job.glue_load_data_to_redshift_job.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.glue_manage_redshift_schema_job.name
      state    = "SUCCEEDED"
    }
  }
}
