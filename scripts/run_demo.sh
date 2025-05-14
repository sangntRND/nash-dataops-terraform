#!/bin/bash
# Demo execution script for the DataOps ETL Demo
# This script runs the ETL pipeline components in sequence

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TERRAFORM_DIR="$PROJECT_ROOT/terraform"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to display messages
log() {
  echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

error() {
  echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: $1${NC}" >&2
}

warn() {
  echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING: $1${NC}"
}

# Check if required tools are installed
check_prerequisites() {
  log "Checking prerequisites..."
  
  if ! command -v aws &> /dev/null; then
    error "AWS CLI is not installed. Please install AWS CLI and try again."
    exit 1
  }
  
  # Check AWS credentials
  if ! aws sts get-caller-identity &> /dev/null; then
    error "AWS credentials not configured or invalid. Please run 'aws configure' and try again."
    exit 1
  }
  
  log "Prerequisites check passed."
}

# Get Terraform outputs
get_terraform_outputs() {
  log "Getting Terraform outputs..."
  
  cd "$TERRAFORM_DIR"
  
  # Get resource names from Terraform outputs
  S3_BUCKET=$(terraform output -raw s3_bucket_name)
  GLUE_DATABASE=$(terraform output -raw glue_database_name)
  RAW_CRAWLER=$(terraform output -raw raw_data_crawler_name)
  GLUE_JOB=$(terraform output -raw glue_job_name)
  PROCESSED_CRAWLER=$(terraform output -raw processed_data_crawler_name)
  
  log "S3 Bucket: $S3_BUCKET"
  log "Glue Database: $GLUE_DATABASE"
  log "Raw Data Crawler: $RAW_CRAWLER"
  log "Glue Job: $GLUE_JOB"
  log "Processed Data Crawler: $PROCESSED_CRAWLER"
}

# Run the Glue crawler for raw data
run_raw_crawler() {
  log "Starting Glue crawler for raw data..."
  
  aws glue start-crawler --name "$RAW_CRAWLER"
  
  # Wait for crawler to complete
  log "Waiting for raw data crawler to complete..."
  while true; do
    status=$(aws glue get-crawler --name "$RAW_CRAWLER" --query 'Crawler.State' --output text)
    if [ "$status" == "READY" ]; then
      break
    fi
    echo -n "."
    sleep 10
  done
  
  log "Raw data crawler completed."
}

# Run the Glue ETL job
run_glue_job() {
  log "Starting Glue ETL job..."
  
  # Get the raw table name
  RAW_TABLES=$(aws glue get-tables --database-name "$GLUE_DATABASE" --query 'TableList[].Name' --output text)
  RAW_TABLE=$(echo "$RAW_TABLES" | grep -i "raw" | head -1)
  
  if [ -z "$RAW_TABLE" ]; then
    error "No raw data table found in the Glue catalog. Crawler may have failed."
    exit 1
  fi
  
  log "Using raw data table: $RAW_TABLE"
  
  # Start the Glue job
  JOB_RUN_ID=$(aws glue start-job-run \
    --job-name "$GLUE_JOB" \
    --arguments "--database_name=$GLUE_DATABASE,--table_name=$RAW_TABLE,--source_location=s3://$S3_BUCKET/raw/,--target_location=s3://$S3_BUCKET/processed/" \
    --query 'JobRunId' --output text)
  
  log "Glue job started with run ID: $JOB_RUN_ID"
  
  # Wait for job to complete
  log "Waiting for Glue job to complete..."
  while true; do
    status=$(aws glue get-job-run --job-name "$GLUE_JOB" --run-id "$JOB_RUN_ID" --query 'JobRun.JobRunState' --output text)
    if [ "$status" == "SUCCEEDED" ]; then
      log "Glue job completed successfully."
      break
    elif [ "$status" == "FAILED" ] || [ "$status" == "TIMEOUT" ] || [ "$status" == "STOPPED" ]; then
      error "Glue job failed with status: $status"
      exit 1
    fi
    echo -n "."
    sleep 10
  done
}

# Run the Glue crawler for processed data
run_processed_crawler() {
  log "Starting Glue crawler for processed data..."
  
  aws glue start-crawler --name "$PROCESSED_CRAWLER"
  
  # Wait for crawler to complete
  log "Waiting for processed data crawler to complete..."
  while true; do
    status=$(aws glue get-crawler --name "$PROCESSED_CRAWLER" --query 'Crawler.State' --output text)
    if [ "$status" == "READY" ]; then
      break
    fi
    echo -n "."
    sleep 10
  done
  
  log "Processed data crawler completed."
}

# Show available tables in Athena
show_available_tables() {
  log "Available tables in Glue Data Catalog:"
  
  aws glue get-tables --database-name "$GLUE_DATABASE" --query 'TableList[].Name' --output table
  
  log "You can now query these tables using Athena or run the sample queries in the examples directory."
}

# Main execution
main() {
  log "Starting DataOps ETL Demo execution..."
  
  check_prerequisites
  get_terraform_outputs
  run_raw_crawler
  run_glue_job
  run_processed_crawler
  show_available_tables
  
  log "Demo execution completed successfully!"
  log "To clean up resources, run: cd $TERRAFORM_DIR && terraform destroy"
}

# Run the main function
main