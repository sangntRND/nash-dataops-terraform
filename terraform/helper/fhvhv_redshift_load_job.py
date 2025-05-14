import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, current_timestamp
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'data_bucket_name',
    'database_name',     # Glue Catalog database
    'redshift_connection',  # The name of the Glue connection to Redshift
    'redshift_database',    # Redshift database name
    'redshift_schema',      # Redshift schema
    'redshift_table',       # Target table in Redshift
    'redshift_host',        # Redshift host (includes port)
    'redshift_username',    # Redshift username
    'redshift_password'     # Redshift password
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get parameters
data_bucket = args['data_bucket_name']
database_name = args['database_name']
redshift_connection = args['redshift_connection']
redshift_database = args['redshift_database']
redshift_schema = args['redshift_schema']
redshift_table = args['redshift_table']
processed_prefix = "processed/fhvhv_trips/"
processed_data_path = f"s3://{data_bucket}/{processed_prefix}"

# Load the processed data from S3
logger.info(f"Reading processed data from: {processed_data_path}")

# Read from Glue Catalog
processed_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name="processed_fhvhv_trips",  # This should match the table name created by the crawler
    transformation_ctx="processed_dyf"
)

# Convert to DataFrame for any final transformations before loading to Redshift
processed_df = processed_dyf.toDF()

# Add a load timestamp
processed_df = processed_df.withColumn("load_timestamp", current_timestamp())

logger.info("Processed data schema before loading to Redshift:")
processed_df.printSchema()

# Count records
record_count = processed_df.count()
logger.info(f"Loading {record_count} records to Redshift table {redshift_schema}.{redshift_table}")

# Extract host and port from endpoint
endpoint_parts = args['redshift_host'].split(':')
host = endpoint_parts[0]
port = int(endpoint_parts[1]) if len(endpoint_parts) > 1 else 5439

# Convert back to DynamicFrame
redshift_dyf = DynamicFrame.fromDF(processed_df, glueContext, "redshift_dyf")

# Updated Redshift connection options with all required parameters
redshift_connection_options = {
    "url": f"jdbc:redshift://{host}:{port}/{redshift_database}",
    "dbtable": f"{redshift_schema}.{redshift_table}",
    "user": args['redshift_username'],
    "password": args['redshift_password'],
    "redshiftTmpDir": f"s3://{data_bucket}/temp/",
    "database": redshift_database,  # Add the required database parameter
    "preactions": f"CREATE SCHEMA IF NOT EXISTS {redshift_schema}"  # Ensure schema exists
}

logger.info(f"Writing data to Redshift table: {redshift_schema}.{redshift_table}")
glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=redshift_dyf,
    catalog_connection=redshift_connection,
    connection_options=redshift_connection_options,
    redshift_tmp_dir=f"s3://{data_bucket}/temp/",
    transformation_ctx="redshift_write"
)

# Log completion and record count
logger.info(f"Successfully loaded {record_count} records to Redshift table {redshift_schema}.{redshift_table}")

# Complete the job
job.commit()
logger.info("Redshift load job completed successfully") 