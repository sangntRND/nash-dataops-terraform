# Nash DataOps Terraform Infrastructure

This repository contains the Infrastructure as Code (IaC) components for the Nash DataOps ETL pipeline using AWS Glue, S3, and Amazon Redshift.

## Architecture

The infrastructure implements a complete data pipeline:

1. **Extract**: Raw data from NYC For-Hire Vehicle (FHV) trips is loaded into S3
2. **Transform**: AWS Glue processes the data, joining with lookup tables and adding derived fields
3. **Load**: Processed data is loaded into Amazon Redshift for analytics

![Architecture Diagram](docs/architecture.md)

## Project Structure

- `/terraform` - Core Terraform configuration files
  - `main.tf` - Core AWS resources and provider configuration
  - `glue_job.tf` - AWS Glue ETL job definitions
  - `redshift_load_job.tf` - Redshift data loading job configuration
  - `glue_crawler.tf` - Glue Crawler configurations for data catalog
  - `variables.tf` - Input variables for customization
  - `terraform.tfvars` - Variable values configuration
  - `/helper` - Supporting scripts and templates
- `/metabase` - Metabase analytics platform configuration with Docker Compose
- `/data` - Sample data files and templates
- `/docs` - Documentation files
- `/scripts` - Deployment and utility scripts

## ETL Workflow

The deployed infrastructure creates an AWS Glue Workflow with the following steps:

1. **S3 to S3 ETL (AWS Glue)**: Transforms raw taxi data, joins with lookup data, and writes processed data back to S3
2. **Redshift Schema Creation (AWS Glue)**: Manages the Redshift database structure, creating tables if they don't exist
3. **S3 to Redshift ETL (AWS Glue)**: Loads processed data into Redshift for analytics

## Setup and Deployment

1. **Prerequisites**:
   - AWS CLI installed and configured
   - Terraform installed (v1.0+)
   - AWS account with appropriate permissions

2. **Deploy Infrastructure**:
   ```bash
   export AWS_PROFILE=cloud-user
   aws s3 mb s3://dataops-glue-etl-demo1-tfstate --region us-east-1
   cd terraform
   terraform init
   terraform apply
   ```

3. **Configure Metabase** (optional):
   ```bash
   cd metabase
   docker-compose up -d
   ```

## Customization

To adapt this project for your own data:
1. Modify the Terraform variables in `terraform.tfvars`
2. Update the Glue job parameters for your specific data sources
3. Adjust Redshift configuration to match your analytics requirements

## Terraform Components

Key infrastructure components defined in this repository:

- S3 buckets for raw and processed data
- AWS Glue ETL jobs for data transformation
- AWS Glue Crawlers for data cataloging
- Redshift cluster for data warehousing
- IAM roles and policies for secure access
- Glue workflow for orchestration
- CloudWatch alarms for monitoring

## Metabase Analytics

The `/metabase` directory contains a Docker Compose setup for running Metabase, an open-source analytics and visualization tool:

- Connects to Redshift to visualize the processed data
- Provides dashboards and ad-hoc query capabilities
- Runs in Docker for easy deployment
- Persistent storage configuration included

## Security Considerations

The infrastructure includes:
- IAM roles with least-privilege permissions
- Secure storage of credentials
- Network isolation for Redshift cluster
- Encryption for data at rest and in transit

## Resource Cleanup

To remove all created resources:

```bash
cd terraform
terraform destroy
```

This will delete all AWS resources created by this project, including S3 buckets, Glue jobs, and the Redshift cluster.

## Related Repositories

- [nash-dataops-pipeline-code](https://github.com/your-org/nash-dataops-pipeline-code) - ETL job code for the pipeline