#!/bin/bash
# Deployment script for the DataOps ETL Demo
# This script initializes and applies the Terraform configuration

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TERRAFORM_DIR="$PROJECT_ROOT/terraform"
DATA_DIR="$PROJECT_ROOT/data"

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
  
  if ! command -v terraform &> /dev/null; then
    error "Terraform is not installed. Please install Terraform and try again."
    exit 1
  fi
  
  if ! command -v aws &> /dev/null; then
    error "AWS CLI is not installed. Please install AWS CLI and try again."
    exit 1
  fi
  
  # Check AWS credentials
  if ! aws sts get-caller-identity &> /dev/null; then
    error "AWS credentials not configured or invalid. Please run 'aws configure' and try again."
    exit 1
  fi
  
  log "Prerequisites check passed."
}

# Create sample data if it doesn't exist
create_sample_data() {
  log "Checking for sample data..."
  
  if [ ! -d "$DATA_DIR" ]; then
    log "Creating data directory..."
    mkdir -p "$DATA_DIR"
  fi
  
  if [ ! -f "$DATA_DIR/sample_data.csv" ]; then
    log "Generating sample data..."
    
    # Create header
    echo "id,name,value,timestamp" > "$DATA_DIR/sample_data.csv"
    
    # Generate 100 rows of sample data
    for i in {1..100}; do
      name="Item-$i"
      value=$((RANDOM % 1000))
      timestamp=$(date -d "2023-01-01 + $i days" +"%Y-%m-%d %H:%M:%S")
      echo "$i,$name,$value,$timestamp" >> "$DATA_DIR/sample_data.csv"
    done
    
    log "Sample data generated at $DATA_DIR/sample_data.csv"
  else
    log "Sample data already exists."
  fi
}

# Initialize and apply Terraform configuration
deploy_infrastructure() {
  log "Deploying infrastructure with Terraform..."
  
  cd "$TERRAFORM_DIR"
  
  # Initialize Terraform
  log "Initializing Terraform..."
  terraform init
  
  # Validate Terraform configuration
  log "Validating Terraform configuration..."
  terraform validate
  
  # Create Terraform plan
  log "Creating Terraform plan..."
  terraform plan -out=tfplan
  
  # Apply Terraform plan
  log "Applying Terraform plan..."
  terraform apply -auto-approve tfplan
  
  # Output Terraform outputs
  log "Deployment completed. Resource information:"
  terraform output
}

# Main execution
main() {
  log "Starting deployment of DataOps ETL Demo..."
  
  check_prerequisites
  create_sample_data
  deploy_infrastructure
  
  log "Deployment completed successfully!"
}

# Run the main function
main
