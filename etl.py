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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Set up logging
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

def perform_full_outer_join(payin_psp_report, session_psp_report):
    """Performs full outer join on the given DataFrames."""
    try:
        # Perform a full outer join on payment_attempt_id
        payin_psp_report_final = payin_psp_report.join(
            session_psp_report,
            on=[payin_psp_report.payment_attempt_id == session_psp_report.payment_attempt_id],
            how='full_outer'
        ).select(
            F.coalesce(payin_psp_report.provider, session_psp_report.provider).alias('psp_provider'),
            F.coalesce(payin_psp_report.payment_attempt_id, session_psp_report.payment_attempt_id).alias('psp_payment_attempt_id'),
            F.coalesce(payin_psp_report.psp_reference_id, session_psp_report.psp_reference_id).alias('psp_psp_reference_id'),
            # Add other columns similar to SQL query
        )

        logger.info("Full outer join completed successfully.")
        return payin_psp_report_final

    except Exception as e:
        logger.error(f"Error in performing full outer join: {str(e)}")
        raise

def prepare_internal_payin_dataframe(provider_filter, payin_filter_date_start, payin_filter_date_end):
    """Prepares the internal payin DataFrame."""
    try:
        internal_payin = datamart_revenue_internal1.filter(
            (F.col("payment_attempt_created_at") > F.lit(payin_filter_date_start)) & 
            (F.col("payment_attempt_created_at") < F.lit(payin_filter_date_end)) &
            (F.length(F.col("payment_attempt_id")) > 0) &
            (F.col("provider") == provider_filter)
        )
        logger.info(f"Internal payin DataFrame prepared for provider: {provider_filter}")
        return internal_payin

    except Exception as e:
        logger.error(f"Error in preparing internal payin DataFrame: {str(e)}")
        raise

def perform_final_join(internal_payin, payin_psp_report_final):
    """Performs final join between internal payin and payin_psp_report_final DataFrames."""
    try:
        final_df = internal_payin.join(
            payin_psp_report_final,
            (F.lower(F.trim(internal_payin.tzp_payment_reference_id)) == F.lower(F.trim(payin_psp_report_final.psp_psp_reference_id))) |
            (F.lower(F.trim(internal_payin.tzp_payin_attempt_id)) == F.lower(F.trim(payin_psp_report_final.psp_payment_attempt_id))),
            how='left'
        )

        final_df = final_df.withColumn(
            "internal_to_psp_map",
            F.when(F.length(payin_psp_report_final.psp_payment_attempt_id) > 0, 1).otherwise(0)
        )

        logger.info("Final join completed successfully.")
        return final_df

    except Exception as e:
        logger.error(f"Error in performing final join: {str(e)}")
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

# Define parameters for each Tazapay PSP provider
provider_configurations = {
    'checkout_sg': {
        'payin_filter_date_start': '2024-07-31',
        'payin_filter_date_end': '2024-08-21',
        'session_filter_date_start': '2024-07-31',
        'session_filter_date_end': '2024-08-21'
    },
    'xendit': {
        'payin_filter_date_start': '2024-05-31',
        'payin_filter_date_end':
        '2024-08-14',
        'session_filter_date_start': '2024-07-31',
        'session_filter_date_end': '2024-08-21'
    }
}

# Process each provider
try:
    for provider, config in provider_configurations.items():
        logger.info(f"Starting processing for provider: {provider}")
        
        final_df = process_provider(
            provider,
            config['payin_filter_date_start'],
            config['payin_filter_date_end'],
            config['session_filter_date_start'],
            config['session_filter_date_end']
        )

        # Write the output to S3 or Glue Catalog
        output_path = f"s3://tazapaybucket/folder/{provider}/"
        final_df.write.mode("overwrite").parquet(output_path)
        logger.info(f"Output written to {output_path} for provider: {provider}")

except Exception as e:
    logger.error(f"An error occurred during the job execution: {str(e)}")

finally:
    job.commit()
    logger.info("Glue job committed successfully.")
