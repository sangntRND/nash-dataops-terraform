import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, lit
from pyspark.sql import DataFrame

# --- Initialization --------------------------------------------------------
# Get job arguments from AWS Glue
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'data_bucket_name'  # S3 bucket for all data
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Configuration ---------------------------------------------------------
# Define S3 paths using the provided bucket name
data_bucket = args['data_bucket_name']
raw_prefix = "raw/"
processed_prefix = "processed/"
validation_prefix = "validation_failures/"

# Path to the processed data that needs validation
processed_data_path = f"s3://{data_bucket}/{processed_prefix}fhvhv_trips/"

# Path to the master data for validation
taxi_zones_path = f"s3://{data_bucket}/{raw_prefix}taxi_zone_lookup.csv"

# Path to write records that fail validation
failure_output_path = f"s3://{data_bucket}/{validation_prefix}fhvhv_trips/"

print(f"Starting data validation for data in: {processed_data_path}")
print(f"Master taxi zone data location: {taxi_zones_path}")
print(f"Validation failure output path: {failure_output_path}")

# --- Data Loading ----------------------------------------------------------
# Load the processed trip data from S3
try:
    print("Loading processed FHVHV trip data...")
    processed_trip_df = spark.read.option("header", "true").csv(processed_data_path)
    # Ensure location IDs are treated as the correct type for joining
    processed_trip_df = processed_trip_df.withColumn("PULocationID", col("PULocationID").cast("integer")) \
                                         .withColumn("DOLocationID", col("DOLocationID").cast("integer"))
except Exception as e:
    print(f"Error loading processed data from {processed_data_path}. Please ensure the path is correct and data exists.")
    raise e

# Load the taxi zone lookup table (master data)
try:
    print("Loading taxi zone lookup data...")
    zones_df = spark.read.option("header", "true").csv(taxi_zones_path)
    # Select and cast the LocationID for accurate joining
    zone_ids_df = zones_df.select(col("LocationID").cast("integer")).distinct()
except Exception as e:
    print(f"Error loading taxi zone lookup data from {taxi_zones_path}.")
    raise e

# --- Validation Logic ------------------------------------------------------
print("Performing validation checks...")

# Rule 1: PULocationID must exist in the master zone lookup table.
# Use a left anti join to find trips where PULocationID is not in the zone lookup.
invalid_pickup_locations_df = processed_trip_df.join(
    zone_ids_df,
    processed_trip_df["PULocationID"] == zone_ids_df["LocationID"],
    "left_anti"
).withColumn("validation_error", lit("Invalid PULocationID"))

# Rule 2: DOLocationID must exist in the master zone lookup table.
# Use a left anti join to find trips where DOLocationID is not in the zone lookup.
invalid_dropoff_locations_df = processed_trip_df.join(
    zone_ids_df,
    processed_trip_df["DOLocationID"] == zone_ids_df["LocationID"],
    "left_anti"
).withColumn("validation_error", lit("Invalid DOLocationID"))

# Rule n: You can add more rules here

# Combine all invalid records into a single DataFrame
# Using unionByName to safely merge DataFrames, even if schema order differs
invalid_records_df = invalid_pickup_locations_df.unionByName(invalid_dropoff_locations_df).distinct()

# --- Validation Outcome ----------------------------------------------------
# Get the total count of records that failed validation
invalid_count = invalid_records_df.count()

if invalid_count > 0:
    print(f"VALIDATION FAILED: Found {invalid_count} records with invalid LocationIDs.")

    # For performance, show a sample of invalid records in the logs
    print("Sample of invalid records:")
    invalid_records_df.show(20, truncate=False)

    # Write the failed records to S3 for analysis
    print(f"Writing {invalid_count} invalid records to: {failure_output_path}")
    invalid_records_df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(failure_output_path)

    # Raise an exception to mark the Glue job as 'Failed'
    raise Exception(f"{invalid_count} records failed validation. See logs and output at {failure_output_path} for details.")
else:
    print("VALIDATION SUCCESSFUL: All PULocationID and DOLocationID values are valid.")

# --- Job Completion --------------------------------------------------------
job.commit()
print("Validation job completed.")