# DataOps Glue ETL Demo

This project demonstrates a complete ETL (Extract, Transform, Load) pipeline using AWS Glue, S3, and Redshift.

## Architecture

The demo implements a complete data pipeline:

1. **Extract**: Raw data from NYC For-Hire Vehicle (FHV) trips is loaded into S3
2. **Transform**: AWS Glue processes the data, joining with lookup tables, adding derived fields
3. **Load**: Processed data is loaded into Amazon Redshift for analytics

![Architecture Diagram](docs/architecture.md)

## Project Structure

- `/terraform` - Infrastructure as Code (Terraform) files
- `/data` - Sample data files
- `/sql` - SQL scripts for Redshift table creation
- `/docs` - Documentation files
- `/scripts` - Deployment and utility scripts

## ETL Process

### 1. S3 to S3 ETL (AWS Glue)

The first ETL job (`fhvhv_etl_job.py`) processes the raw taxi data:

- Reads Parquet files from S3
- Joins with taxi zone lookup data 
- Derives time-based dimensions
- Calculates trip metrics
- Writes processed CSV data back to S3

### 2. Redshift Schema Creation (AWS Glue)

The schema creation job (`redshift_create_schema_job.py`) intelligently manages the Redshift database structure:

- Checks if schema and tables already exist before attempting creation
- Only creates schema and tables if they don't already exist
- Uses DO blocks and IF NOT EXISTS clauses for idempotent execution
- Always updates views to ensure they reflect the latest structure
- Logs execution status and detailed reports to S3
- This job runs automatically after the S3 ETL job completes

### 3. S3 to Redshift ETL (AWS Glue)

The Redshift load job (`fhvhv_redshift_load_job.py`) loads data into Redshift:

- Reads processed CSV data from S3
- Adds load timestamp for tracking
- Loads data into Redshift table
- Uses Redshift's columnar storage for efficient analytics
- This job runs automatically after the schema creation job completes

## Workflow Orchestration

The ETL process is orchestrated using AWS Glue Workflows:

1. The workflow starts on a scheduled basis (daily at midnight UTC)
2. The S3 ETL job transforms raw data and writes to S3 as CSV
3. Upon successful completion, it triggers the Redshift schema creation job
4. The schema job checks if database objects exist and creates them if needed
5. Once schemas are verified, the Redshift load job is triggered
6. The entire workflow runs automatically as a single unit

This ensures that:
- Tables exist before data load attempts
- Tables are not dropped and recreated unnecessarily
- Data integrity is preserved through idempotent operations
- Data is transformed before loading
- Dependencies are properly managed
- Errors in any step prevent downstream steps from executing

## Idempotent Operations

The ETL pipeline is designed to be idempotent - it can be run multiple times without causing harm:

- The schema creation job checks if objects exist before creating them
- SQL statements use IF NOT EXISTS clauses
- The data loading uses TRUNCATE or APPEND modes to avoid duplicating data
- The job logging keeps track of execution history

This makes the pipeline:
- Safe to run on a schedule
- Safe to re-run after failures
- Suitable for production environments

## Redshift Analytics

The processed data is loaded into a Redshift cluster with:

- Optimized table structure (DISTKEY and SORTKEY)
- Analytics views for common query patterns
- Time-based partitioning for efficient querying

## Setup and Deployment

1. **Prerequisites**:
   - AWS CLI installed and configured
   - Terraform installed
   - AWS account with appropriate permissions

2. **Deploy Infrastructure**:
   ```bash
   cd terraform
   terraform init
   terraform apply
   ```

3. **Initialize Redshift**:
   - Run the SQL script in Redshift to create tables:
   ```bash
   aws s3 cp s3://[YOUR_BUCKET]/scripts/create_redshift_tables.sql .
   # Connect to Redshift and run the script
   ```

4. **Run ETL Pipeline**:
   ```bash
   # Run the ETL workflow
   aws glue start-workflow-run --name fhvhv-etl-workflow-dev
   ```

## Features

- **Data Cleansing**: Handles null values and data type conversions
- **Data Enrichment**: Joins with lookup data, adds geographic context
- **Performance Optimization**: Partitioned data, efficient Redshift table design
- **Observability**: Logging, metrics, and execution tracking
- **Orchestration**: Workflow with job dependencies

## Customization

To adapt this demo for your own data:
1. Modify the ETL scripts in `/terraform/helper`
2. Update the SQL schema in `/sql/create_redshift_tables.sql`
3. Adjust Terraform variables in `terraform.tfvars`

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/dataops-glue-etl-demo.git
cd dataops-glue-etl-demo
```

### 2. Deploy the Infrastructure

Run the deployment script:

```bash
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```

This script will:
- Check prerequisites
- Generate sample data if needed
- Initialize and apply the Terraform configuration

### 3. Run the Demo

Execute the demo script:

```bash
chmod +x scripts/run_demo.sh
./scripts/run_demo.sh
```

This script will:
- Run the Glue Crawler to catalog raw data
- Execute the Glue ETL job to transform the data
- Run another Glue Crawler to catalog processed data
- Display available tables for querying

### 4. Query the Data

You can query the processed data using Amazon Athena. Sample queries are provided in the `examples/sample_queries.sql` file.

## Project Structure

```
dataops-glue-etl-demo/
├── data/                  # Sample data directory
├── examples/              # Example queries and usage
├── scripts/               # Deployment and execution scripts
├── terraform/             # Terraform configuration files
│   ├── glue_crawler.tf    # Glue crawler configuration
│   ├── glue_job.tf        # Glue job configuration
│   ├── glue_job.py        # Glue ETL job script
│   ├── lambda_ingestion.tf # Lambda for data ingestion
│   ├── main.tf            # Main Terraform configuration
│   ├── outputs.tf         # Output definitions
│   └── variables.tf       # Input variables
└── README.md              # Project documentation
```

## FHVHV Trip Data ETL Job

This project includes a dedicated ETL job for processing For-Hire Vehicle (FHV) High Volume trip data from the NYC Taxi & Limousine Commission (TLC).

### Data Sources

The job processes two primary data sources:
1. **fhvhv_tripdata_2024-01.parquet** - Contains high volume for-hire vehicle trip records for January 2024
2. **taxi_zone_lookup.csv** - Reference data that maps location IDs to NYC boroughs and zones

### ETL Process

The FHVHV ETL job performs the following transformations:

1. **Data Loading**: Reads raw Parquet and CSV files from the S3 raw data directory
2. **Date/Time Handling**: Converts string timestamps to proper timestamp data types
3. **Data Enrichment**: Joins trip data with taxi zone lookup data to add borough and zone information
4. **Shared Ride Processing**: Creates a human-readable status field based on the shared_match_flag column
5. **Processing Metadata**: Adds processing timestamp for data lineage tracking
6. **Data Partitioning**: Writes the output partitioned by pickup borough for optimized querying
7. **Format Optimization**: Stores processed data in Parquet format for efficient storage and querying

### Job Configuration

The job is configured with the following parameters:
- 2 G.1X workers (4 vCPU, 16GB memory each)
- Glue version 3.0
- Daily run schedule at 1:00 AM UTC
- Job bookmarks enabled for incremental processing
- Spark UI and metrics enabled for monitoring

### Output Data

Processed data is stored in:
```
s3://<your-bucket>/processed/fhvhv_trips/
```

The data is partitioned by pickup borough (PU_Borough), which optimizes queries filtered by geographic region.

### Usage Examples

Query the processed data using Amazon Athena. Example query to analyze trip patterns by borough:

```sql
SELECT 
  PU_Borough,
  COUNT(*) AS trip_count,
  AVG(trip_distance) AS avg_distance,
  COUNT(CASE WHEN shared_ride_status = 'Shared Ride' THEN 1 END) AS shared_rides
FROM 
  processed_fhvhv_trips
GROUP BY 
  PU_Borough
ORDER BY 
  trip_count DESC;
```

## Customization

To customize this demo for your own data:

1. Replace the sample data in the `data/` directory with your own data
2. Modify the Glue ETL job script (`terraform/glue_job.py`) to implement your transformations
3. Update the Terraform variables as needed

## Cleanup

To remove all resources created by this demo:

```bash
cd terraform
terraform destroy
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Draft command:
export AWS_PROFILE=cloud-user
aws s3 mb s3://dataops-glue-etl-demo1-tfstate --region us-east-1

## RFVAR (Reduced Form Vector Autoregression) Analysis

This project now includes RFVAR analysis capabilities for the processed taxi trip data. RFVAR is a statistical method used to analyze relationships between multiple time series variables and generate forecasts.

### What is RFVAR?

RFVAR (Reduced Form Vector Autoregression) is a statistical model that:

1. Captures temporal dependencies in multivariate time series data
2. Models each variable as a function of past values of itself AND past values of other variables
3. Enables forecasting of future values based on historical patterns
4. Provides insights into how variables impact each other over time through Impulse Response Functions (IRFs)

### RFVAR Functionality in this Project

The RFVAR component in this project:

1. Aggregates taxi trip data to daily level (trip counts, total distance, fare amounts, average duration)
2. Preprocesses data (handles missing values, standardizes variables)
3. Fits a VAR model with configurable lag parameters
4. Generates forecasts for future periods
5. Calculates confidence intervals for forecasts
6. Computes impulse response functions to analyze variable relationships
7. Stores results in S3 for querying with Athena

### Using the RFVAR Functionality

The RFVAR job is automatically triggered after the main ETL job completes. Results are stored in:
- `s3://<your-bucket>/rfvar_analysis/forecasts/` - Future value predictions
- `s3://<your-bucket>/rfvar_analysis/irf/` - Impulse response functions
- `s3://<your-bucket>/rfvar_analysis/model_info/` - Model parameters and metrics

### Querying RFVAR Results

Sample queries for the RFVAR results are provided in the `examples/rfvar_queries.sql` file. These include:

1. Viewing forecast values with confidence intervals
2. Analyzing impulse response functions
3. Calculating forecast accuracy metrics
4. Comparing different forecast versions

### Customizing RFVAR Parameters

You can customize the RFVAR model by modifying the following parameters in the Terraform configuration:

```terraform
# In terraform/rfvar_job.tf
default_arguments = {
  # ... other arguments
  "--lags"            = "3"  # Number of lags in the VAR model
  "--forecast_horizon" = "7"  # Number of periods to forecast
}
```

Increasing the lags captures more historical dependencies but requires more data and can lead to overfitting. The forecast horizon determines how many periods ahead are predicted.

### Interpreting RFVAR Results

1. **Forecasts**: These represent predicted future values for each variable.
2. **Confidence Intervals**: Provide a range of possible values, indicating forecast uncertainty.
3. **Impulse Response Functions**: Show how a one-standard-deviation shock in one variable affects others over time.

For example, an IRF might show that a spike in total fare amounts predicts an increase in trip counts 2 days later, followed by a gradual decline.
