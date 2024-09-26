"""
Author: Sandeep R Diddi
Date: 2024-09-04
Description: AWS Glue job script for dynamic ETL processing for multiple PSP.

Change Log:
Date        Author        Description
----------  ------------  -----------------------------------------------------
2024-09-04  Sandeep     Initial script creation with dynamic provider processing and error handling.
2024-09-05  Sandeep     Added data quality checks and improved logging.
2024-09-26  Sandeep     Modified script to accept dynamic date inputs at runtime.
"""

import sys
import logging
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException

# Initialize Glue Context and set up logging
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'PROVIDER', 'PAYIN_FILTER_DATE_START', 'PAYIN_FILTER_DATE_END', 'SESSION_FILTER_DATE_START', 'SESSION_FILTER_DATE_END'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set up logging on GLUE
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def load_data(database: str, table_name: str) -> DataFrame:
    """Loads data from Glue Data Catalog."""
    try:
        df = glueContext.create_dynamic_frame.from_catalog(
            database=database,
            table_name=table_name
        ).toDF()
        logger.info(f"Loaded data from {database}.{table_name} successfully.")
        return df
    except AnalysisException as e:
        logger.error(f"Error loading data from {database}.{table_name}: {str(e)}")
        raise

def data_quality_checks(df: DataFrame, check_columns: list) -> bool:
    """Performs basic data quality checks."""
    if df is None or df.rdd.isEmpty():
        logger.error("DataFrame is empty!")
        return False
    for column in check_columns:
        if column not in df.columns:
            logger.error(f"Column {column} not found in DataFrame!")
            return False
    logger.info("Data quality checks passed.")
    return True

def prepare_payin_session_dataframes(payin_filter_date_start, payin_filter_date_end, session_filter_date_start, session_filter_date_end):
    """Prepares the payin and session DataFrames with required transformations."""
    try:
        # Filter payin_psp_report dataframe
        payin_psp_report = payin_psp_report_homo.filter(
            (F.col("created_at_date") > F.lit(payin_filter_date_start)) & 
            (F.col("created_at_date") < F.lit(payin_filter_date_end))
        )

        # Filter session_psp_report dataframe
        session_psp_report = session_psp_report_homo.filter(
            (F.col("created_at_date") > F.lit(session_filter_date_start)) & 
            (F.col("created_at_date") < F.lit(session_filter_date_end))
        )

        # Perform calculations on payin_psp_report
        payin_psp_report = payin_psp_report.withColumn(
            "txn_fee",
            (F.coalesce(F.col("interchange_fee"), F.lit(0)) +
             F.coalesce(F.col("scheme_fixed_fee"), F.lit(0)) +
             F.coalesce(F.col("scheme_variable_fee"), F.lit(0)) +
             F.coalesce(F.col("processor_fixed_fee"), F.lit(0)) +
             F.coalesce(F.col("processor_variable_fee"), F.lit(0)) +
             F.coalesce(F.col("authorization_fee"), F.lit(0)) +
             F.coalesce(F.col("void_fee"), F.lit(0)) +
             F.coalesce(F.col("misc_fee"), F.lit(0)) +
             F.coalesce(F.col("vat"), F.lit(0))) * -1
        ).withColumnRenamed("3ds_fee", "three_ds_fee")

        # Perform calculations on session_psp_report
        session_psp_report = session_psp_report.withColumn(
            "authen_fee",
            (F.coalesce(F.col("interchange_fee"), F.lit(0)) +
             F.coalesce(F.col("scheme_fixed_fee"), F.lit(0)) +
             F.coalesce(F.col("scheme_variable_fee"), F.lit(0)) +
             F.coalesce(F.col("void_fee"), F.lit(0))) * -1
        )

        logger.info("DataFrames prepared with required transformations.")
        return payin_psp_report, session_psp_report

    except Exception as e:
        logger.error(f"Error in preparing DataFrames: {str(e)}")
        raise

def process_provider(provider_name, payin_filter_date_start, payin_filter_date_end, session_filter_date_start, session_filter_date_end):
    """Executes the process for a given provider."""
    try:
        # Prepare dataframes
        payin_psp_report, session_psp_report = prepare_payin_session_dataframes(
            payin_filter_date_start, payin_filter_date_end, session_filter_date_start, session_filter_date_end
        )

        # Data Quality Checks
        if not data_quality_checks(payin_psp_report, ['payment_attempt_id', 'provider']) or \
           not data_quality_checks(session_psp_report, ['payment_attempt_id', 'provider']):
            raise ValueError("Data quality checks failed!")

        # Perform outer join
        payin_psp_report_final = perform_full_outer_join(payin_psp_report, session_psp_report)

        # Prepare internal payin dataframe
        internal_payin = prepare_internal_payin_dataframe(provider_name, payin_filter_date_start, payin_filter_date_end)

        # Perform final join and return the result
        final_df = perform_final_join(internal_payin, payin_psp_report_final)

        # Data Quality Checks on final dataframe
        if not data_quality_checks(final_df, ['tzp_payin_attempt_id', 'internal_to_psp_map']):
            raise ValueError("Final DataFrame data quality checks failed!")

        return final_df

    except Exception as e:
        logger.error(f"Error processing provider {provider_name}: {str(e)}")
        raise

# Process the provider dynamically based on the runtime parameters
try:
    logger.info(f"Starting processing for provider: {args['PROVIDER']}")
    
    final_df = process_provider(
        args['PROVIDER'],
        args['PAYIN_FILTER_DATE_START'],
        args['PAYIN_FILTER_DATE_END'],
        args['SESSION_FILTER_DATE_START'],
        args['SESSION_FILTER_DATE_END']
    )

    # Write the output to S3 or Glue Catalog
    output_path = f"s3://tazapaybucket/folder/{args['PROVIDER']}/"
    final_df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Output written to {output_path} for provider: {args['PROVIDER']}")

except Exception as e:
    logger.error(f"An error occurred during the job execution: {str(e)}")

finally:
    job.commit()
    logger.info("Glue job committed successfully.")
