import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, year, month, lit, regexp_replace

# --- Initialization ---
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'data_bucket_name',
    'database_name',
    'fhvhv_table_name'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Configuration ---
data_bucket = args['data_bucket_name']
database_name = args['database_name']
table_name = args['fhvhv_table_name']
raw_prefix = "raw/"
processed_prefix = "processed/"

taxi_zones_path = f"s3://{data_bucket}/{raw_prefix}taxi_zone_lookup.csv"
output_path = f"s3://{data_bucket}/{processed_prefix}fhvhv_trips/"

# --- Data Loading ---
print(f"Loading raw data from table: {database_name}.{table_name}")
raw_trip_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=database_name,
    table_name=table_name,
    transformation_ctx="raw_trip_dyf"
)
raw_trip_df = raw_trip_dyf.toDF()

print(f"Loading taxi zone lookup data from: {taxi_zones_path}")
zones_df = spark.read.option("header", "true").csv(taxi_zones_path)

# --- Data Transformation ---
pu_zones_df = zones_df.withColumnRenamed("LocationID", "PULocationID") \
                      .withColumnRenamed("Borough", "PU_Borough") \
                      .withColumnRenamed("Zone", "PU_Zone") \
                      .withColumnRenamed("service_zone", "PU_service_zone")

do_zones_df = zones_df.withColumnRenamed("LocationID", "DOLocationID") \
                      .withColumnRenamed("Borough", "DO_Borough") \
                      .withColumnRenamed("Zone", "DO_Zone") \
                      .withColumnRenamed("service_zone", "DO_service_zone")

enriched_df = raw_trip_df.join(
    pu_zones_df,
    raw_trip_df["PULocationID"] == pu_zones_df["PULocationID"],
    "left"
).drop(pu_zones_df["PULocationID"])

enriched_df = enriched_df.join(
    do_zones_df,
    enriched_df["DOLocationID"] == do_zones_df["DOLocationID"],
    "left"
).drop(do_zones_df["DOLocationID"])

# --- Data Validation and Filtering ---
pre_filter_count = enriched_df.count()
print(f"Total records after joins: {pre_filter_count}")

enriched_df = enriched_df.filter(
    col("PU_Borough").isNotNull() & col("DO_Borough").isNotNull()
)

post_filter_count = enriched_df.count()
print(f"Records dropped due to invalid LocationID: {pre_filter_count - post_filter_count}")
print(f"Total valid records to be stored: {post_filter_count}")


# --- Data Cleaning and Partitioning ---
df_with_partitions = enriched_df.withColumn("pickup_date", to_date(col("pickup_datetime")))
df_with_partitions = df_with_partitions.withColumn("pickup_year", year(col("pickup_date"))) \
                                       .withColumn("pickup_month", month(col("pickup_date")))

# 1. Filter out records where partition key values are null or empty to prevent __HIVE_DEFAULT_PARTITION__
final_df = df_with_partitions.filter(
    col("pickup_year").isNotNull() &
    col("pickup_month").isNotNull() &
    col("PU_Borough").isNotNull() & (col("PU_Borough") != "") &
    col("PU_Zone").isNotNull() & (col("PU_Zone") != "")
)

# 2. Sanitize string-based partition columns to remove special characters
final_df = final_df.withColumn("PU_Borough_sanitized", regexp_replace(col("PU_Borough"), r"[^a-zA-Z0-9 ]", "_")) \
                   .withColumn("PU_Zone_sanitized", regexp_replace(col("PU_Zone"), r"[^a-zA-Z0-9 ]", "_"))

print(f"Final record count after all cleaning: {final_df.count()}")

# --- Data Storage ---
# Write the processed and validated data to S3, partitioned by the sanitized columns
print(f"Writing processed data to: {output_path}")
final_df.write \
    .mode("overwrite") \
    .partitionBy("pickup_year", "pickup_month", "PU_Borough_sanitized", "PU_Zone_sanitized") \
    .csv(output_path, header=True)

# --- Job Completion ---
job.commit()
print("Job completed successfully.")