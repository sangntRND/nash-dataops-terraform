import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Mapping from Glue/Spark types to Redshift types
TYPE_MAPPING = {
    'string': 'VARCHAR(256)',
    'long': 'BIGINT',
    'int': 'INTEGER',
    'double': 'DOUBLE PRECISION',
    'float': 'REAL',
    'boolean': 'BOOLEAN',
    'binary': 'BYTEA',
    'timestamp': 'TIMESTAMP',
    'date': 'DATE',
    'array': 'VARCHAR(65535)',  # Arrays stored as JSON strings
    'map': 'VARCHAR(65535)',    # Maps stored as JSON strings
    'struct': 'VARCHAR(65535)'  # Structs stored as JSON strings
}

def get_redshift_type(glue_type):
    """Convert Glue type to Redshift type"""
    base_type = glue_type.lower()
    return TYPE_MAPPING.get(base_type, 'VARCHAR(256)')  # Default to VARCHAR if type unknown

def create_table_ddl(schema_name, table_name, schema_fields, dist_key=None, sort_key=None):
    """Generate Redshift CREATE TABLE DDL"""
    column_definitions = []
    
    for field in schema_fields:
        col_name = field.name.lower()
        col_type = get_redshift_type(str(field.dataType))
        column_definitions.append(f"{col_name} {col_type}")
    
    # Add load_timestamp column
    column_definitions.append("load_timestamp TIMESTAMP")
    
    # Add distribution and sort keys if specified
    dist_style = f"DISTSTYLE KEY DISTKEY({dist_key})" if dist_key else "DISTSTYLE AUTO"
    sort_style = f"SORTKEY({sort_key})" if sort_key else ""
    
    # Join column definitions with newlines without using f-string
    columns_sql = ',\n        '.join(column_definitions)
    
    # Create the DDL statement
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
        {columns_sql}
    )
    {dist_style}
    {sort_style};
    """
    
    return ddl

def check_schema_exists(conn, schema_name):
    """Check if a schema exists in Redshift"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{schema_name}'")
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except Exception as e:
        logger.error(f"Error checking schema existence: {str(e)}")
        return False

def check_table_exists(conn, schema_name, table_name):
    """Check if a table exists in Redshift"""
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'")
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except Exception as e:
        logger.error(f"Error checking table existence: {str(e)}")
        return False

def main():
    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'database_name',          # Glue Catalog database
        'table_name',            # Glue Catalog table
        'redshift_connection',    # Redshift connection name
        'redshift_database',      # Redshift database
        'redshift_schema',        # Redshift schema
        'redshift_table',        # Redshift table
        'redshift_host',         # Redshift host (includes port)
        'redshift_username',     # Redshift username
        'redshift_password'      # Redshift password
    ])

    # Initialize Glue context
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)

    try:
        # Get the source table schema from Glue Catalog
        logger.info(f"Getting schema for {args['database_name']}.{args['table_name']}")
        dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database=args['database_name'],
            table_name=args['table_name']
        )
        
        schema = dynamic_frame.schema()
        if not schema:
            raise Exception("Failed to get schema from Glue Catalog")

        # Extract host and port from endpoint
        endpoint_parts = args['redshift_host'].split(':')
        host = endpoint_parts[0]
        port = int(endpoint_parts[1]) if len(endpoint_parts) > 1 else 5439  # Default to 5439 if no port specified
        
        # Connect to Redshift
        logger.info(f"Connecting to Redshift: {host}:{port}")
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=args['redshift_database'],
            user=args['redshift_username'],
            password=args['redshift_password']
        )
        conn.autocommit = True

        # Create schema if it doesn't exist
        if not check_schema_exists(conn, args['redshift_schema']):
            logger.info(f"Creating schema {args['redshift_schema']}")
            cursor = conn.cursor()
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {args['redshift_schema']}")
            cursor.close()

        # Generate and execute CREATE TABLE DDL
        # We'll use pickup_date as DISTKEY and (pickup_date, pu_borough) as SORTKEY
        # These are good choices for typical taxi data queries
        ddl = create_table_ddl(
            args['redshift_schema'],
            args['redshift_table'],
            schema.fields,
            dist_key='pickup_date',
            sort_key='pickup_date, pu_borough'
        )

        logger.info("Executing CREATE TABLE statement:")
        logger.info(ddl)
        
        cursor = conn.cursor()
        cursor.execute(ddl)
        cursor.close()
        
        # Create useful views
        # views_ddl = f"""
        # -- View for latest data
        # CREATE OR REPLACE VIEW {args['redshift_schema']}.{args['redshift_table']}_latest AS
        # SELECT *
        # FROM {args['redshift_schema']}.{args['redshift_table']}
        # WHERE DATE(load_timestamp) = (
        #     SELECT MAX(DATE(load_timestamp)) 
        #     FROM {args['redshift_schema']}.{args['redshift_table']}
        # );

        # -- View for borough-level aggregates
        # CREATE OR REPLACE VIEW {args['redshift_schema']}.{args['redshift_table']}_by_borough AS
        # SELECT 
        #     pickup_date,
        #     pu_borough,
        #     COUNT(*) as trip_count,
        #     AVG(trip_duration_minutes) as avg_trip_duration,
        #     SUM(CASE WHEN shared_ride_status = 'Shared Ride' THEN 1 ELSE 0 END) as shared_rides
        # FROM {args['redshift_schema']}.{args['redshift_table']}
        # GROUP BY pickup_date, pu_borough
        # ORDER BY pickup_date, pu_borough;

        # -- View for hourly patterns
        # CREATE OR REPLACE VIEW {args['redshift_schema']}.{args['redshift_table']}_by_hour AS
        # SELECT 
        #     pickup_hour,
        #     pu_borough,
        #     COUNT(*) as trip_count,
        #     AVG(trip_duration_minutes) as avg_trip_duration
        # FROM {args['redshift_schema']}.{args['redshift_table']}
        # GROUP BY pickup_hour, pu_borough
        # ORDER BY pickup_hour, pu_borough;
        # """

        # logger.info("Creating views...")
        # cursor = conn.cursor()
        # cursor.execute(views_ddl)
        # cursor.close()

        conn.close()
        logger.info("Schema creation completed successfully")

    except Exception as e:
        logger.error(f"Error in job execution: {str(e)}")
        raise e

    finally:
        job.commit()

if __name__ == "__main__":
    main() 