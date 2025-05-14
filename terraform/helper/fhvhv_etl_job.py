import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_timestamp, lit, when, year, month, dayofmonth, hour, date_format, unix_timestamp

# Initialize Glue context
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 
    'data_bucket_name',
    'database_name',    # Database in Glue Catalog
    'fhvhv_table_name'  # Table name for fhvhv data in Glue Catalog
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get parameters
data_bucket = args['data_bucket_name']
database_name = args['database_name']
fhvhv_table_name = args['fhvhv_table_name']
processed_prefix = "processed/"
raw_prefix = "raw/"
taxi_zones_path = f"s3://{data_bucket}/{raw_prefix}taxi_zone_lookup.csv"
output_path = f"s3://{data_bucket}/{processed_prefix}fhvhv_trips/"

# No need for JDBC extraction for Parquet data
print(f"Working with Parquet data from table {fhvhv_table_name} in database {database_name}")

# Load data from Glue Data Catalog directly
print(f"Loading FHVHV trip data from catalog: {database_name}.{fhvhv_table_name}")
trip_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=fhvhv_table_name,
    transformation_ctx="trip_dyf"
)

# Get schema information
print(f"Table information for {database_name}.{fhvhv_table_name}:")
schema = trip_dyf.schema()
if schema:
    for field in schema.fields:
        print(f"Column: {field.name}, Type: {field.dataType}")
else:
    print("Schema information not available")

# Convert DynamicFrame to DataFrame for data processing
trip_df = trip_dyf.toDF()
print("FHVHV Trip Data Schema:")
trip_df.printSchema()

print("Loading taxi zone lookup data from:", taxi_zones_path)
zones_df = spark.read.option("header", "true").csv(taxi_zones_path)

# Data cleanup and transformation
print("Performing data transformations and cleanup...")

# Convert string timestamps to timestamp type and handle potential null values
print("Converting timestamp columns...")
trip_df = trip_df.withColumn("pickup_datetime", to_timestamp(col("pickup_datetime"))) \
                .withColumn("dropoff_datetime", to_timestamp(col("dropoff_datetime")))

# Add derived time dimension columns for better analytics and partitioning
print("Adding time dimension columns...")
trip_df = trip_df.withColumn("pickup_year", year("pickup_datetime")) \
                .withColumn("pickup_month", month("pickup_datetime")) \
                .withColumn("pickup_day", dayofmonth("pickup_datetime")) \
                .withColumn("pickup_hour", hour("pickup_datetime")) \
                .withColumn("pickup_date", date_format("pickup_datetime", "yyyy-MM-dd"))

# Join with taxi zone lookup for pickup location
print("Joining with taxi zone data for pickup locations...")
trip_df = trip_df.join(
    zones_df.select(
        col("LocationID").alias("PU_LocationID"),
        col("Borough").alias("PU_Borough"),
        col("Zone").alias("PU_Zone"),
        col("service_zone").alias("PU_service_zone")
    ),
    trip_df["PULocationID"] == col("PU_LocationID"),
    "left"
).drop("PU_LocationID")

# Join with taxi zone lookup for dropoff location
print("Joining with taxi zone data for dropoff locations...")
trip_df = trip_df.join(
    zones_df.select(
        col("LocationID").alias("DO_LocationID"),
        col("Borough").alias("DO_Borough"),
        col("Zone").alias("DO_Zone"),
        col("service_zone").alias("DO_service_zone")
    ),
    trip_df["DOLocationID"] == col("DO_LocationID"),
    "left"
).drop("DO_LocationID")

# Handle shared ride flags - using sr_flag from the schema
print("Adding shared ride status column...")
trip_df = trip_df.withColumn(
    "shared_ride_status",
    when(col("sr_flag") == 1, "Shared Ride")
    .otherwise("Non-Shared Ride")
)

# Add a process datetime column for data lineage tracking
trip_df = trip_df.withColumn("processed_date", lit(spark.sql("SELECT CURRENT_TIMESTAMP").collect()[0][0]))

# Calculate trip duration in minutes (for analytics purposes)
print("Calculating trip metrics...")
trip_df = trip_df.withColumn(
    "trip_duration_minutes", 
    when(
        col("pickup_datetime").isNotNull() & col("dropoff_datetime").isNotNull(),
        (unix_timestamp("dropoff_datetime") - unix_timestamp("pickup_datetime")) / 60
    ).otherwise(None)
)

# Write processed data to S3, partitioning by year, month and borough for efficient querying
print(f"Writing processed data to: {output_path}")
trip_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .option("quoteAll", "true") \
    .option("encoding", "UTF-8") \
    .option("delimiter", ",") \
    .partitionBy("pickup_year", "pickup_month", "PU_Borough", "PU_Zone") \
    .csv(output_path)

# Print job statistics
# record_count = trip_df.count()
# print(f"Processed {record_count} FHV trip records")

# Complete the job
job.commit()
print("FHVHV ETL job completed successfully") 