import sys
import logging
import boto3
import pandas as pd
from io import StringIO
from datetime import datetime
from botocore.exceptions import ClientError
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Define parameters
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'athena_database_name',
        'output_path',
        'cdc_path',
        'load_type',
        'primary_key_s3_prefix'
    ]
)

athena_database_name = args['athena_database_name']
output_path = args['output_path']
cdc_path = args['cdc_path']
load_type = args['load_type'].lower()
primary_key_s3_prefix = args['primary_key_s3_prefix']

# Initialize AWS clients
s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

# Helper function to fetch the latest primary key CSV
def get_latest_primary_key_csv():
    bucket_name, prefix = primary_key_s3_prefix.replace("s3://", "").split("/", 1)
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    csv_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".csv")]
    latest_csv = max(csv_files) if csv_files else None
    if latest_csv:
        response = s3_client.get_object(Bucket=bucket_name, Key=latest_csv)
        primary_key_csv_data = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(primary_key_csv_data))
    else:
        raise FileNotFoundError("No primary key CSV file found in specified S3 location.")

# Load primary key information for CDC 
primary_key_df = get_latest_primary_key_csv()

def get_primary_key(table_name):
    row = primary_key_df[primary_key_df['table_name'] == table_name]
    if not row.empty:
        return row['primary_key_column'].values[0]
    else:
        logger.warning(f"No primary key found for table {table_name}. Skipping.")
        return None

def get_cdc_tracker_path(table_name):
    return f"{cdc_path}/{table_name}.txt"

def normalize_timestamp(timestamp_str):
    try:
        dt = datetime.strptime(timestamp_str.split('+')[0], "%Y-%m-%d %H:%M:%S.%f")
        return dt.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    except ValueError:
        return None

def get_last_updated_timestamp(table_name):
    cdc_tracker_path = get_cdc_tracker_path(table_name)
    try:
        last_timestamp_df = spark.read.text(cdc_tracker_path)
        raw_last_timestamp = last_timestamp_df.collect()[0][0]
        return normalize_timestamp(raw_last_timestamp)
    except:
        return None

def update_last_updated_timestamp(table_name, max_timestamp):
    cdc_tracker_path = get_cdc_tracker_path(table_name)
    max_timestamp_str = max_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    timestamp_df = spark.createDataFrame([(max_timestamp_str,)], ["timestamp"])
    timestamp_df.write.mode("overwrite").text(cdc_tracker_path)
    logger.info(f"Updated CDC tracker for {table_name}.")

#This helps to solve the error when building Mart which looks
#for PRQ files 
def update_glue_partitions(database_name, table_name, s3_path, partitions):
    glue_client.batch_create_partition(
        DatabaseName=database_name,
        TableName=table_name,
        PartitionInputList=[{
            "Values": partitions,
            "StorageDescriptor": {
                "Location": s3_path,
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                }
            }
        }]
    )

tables_response = glue_client.get_tables(DatabaseName=athena_database_name)
table_names = [table['Name'] for table in tables_response['TableList']]

# Process each table
for table_name in table_names:
    logger.info(f"Processing table: {table_name}")
    last_updated = get_last_updated_timestamp(table_name)

    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database=athena_database_name, table_name=table_name
    )

    df = dynamic_frame.toDF()
    if 'updated_at' not in df.columns:
        logger.warning(f"'updated_at' column not found in {table_name}. Skipping.")
        continue

    df = df.withColumn("normalized_updated_at", df["updated_at"].cast("string"))
    df = df.withColumn("normalized_updated_at", df["normalized_updated_at"].substr(1, 23))

    primary_key = get_primary_key(table_name)
    if not primary_key:
        continue

    # Generate dynamic path
    current_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    s3_table_path = f"{output_path}/{table_name}/{current_timestamp}/"

    if load_type == "full" or last_updated is None:
        df_deduped = df.dropDuplicates([primary_key])
        df_deduped.write.mode("overwrite").partitionBy("updated_at").parquet(s3_table_path)
        max_timestamp = df_deduped.agg({"updated_at": "max"}).collect()[0][0]
    else:
        df_filtered = df.filter(df['normalized_updated_at'] > last_updated)
        if df_filtered.count() > 0:
            df_filtered.write.mode("append").partitionBy("updated_at").parquet(s3_table_path)
            max_timestamp = df_filtered.agg({"updated_at": "max"}).collect()[0][0]
        else:
            logger.info(f"No new data for {table_name}. Skipping.")
            continue

    update_last_updated_timestamp(table_name, max_timestamp)
    update_glue_partitions(athena_database_name, table_name, s3_table_path, [current_timestamp])

job.commit()
logger.info("Job completed successfully.")
