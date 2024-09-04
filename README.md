# AWS Glue Job for Dynamic ETL Processing

This repository contains a PySpark script for an AWS Glue job designed to dynamically handle ETL (Extract, Transform, Load) processes for multiple service providers. The job script leverages AWS Glue's capabilities to process large-scale datasets stored in AWS Glue Data Catalog and S3.

## Table of Contents

1. [Overview](#overview)
2. [Prerequisites](#prerequisites)
3. [Script Description](#script-description)
4. [Configuration](#configuration)
5. [Execution](#execution)
6. [Error Handling and Logging](#error-handling-and-logging)
7. [Data Quality Checks](#data-quality-checks)
8. [Extending the Script](#extending-the-script)
9. [Best Practices](#best-practices)
10. [Conclusion](#conclusion)

## Overview

The AWS Glue job script is designed to:

- Dynamically process and transform data for multiple service providers (e.g., `checkout_sg`, `xendit`).
- Perform data quality checks to ensure the integrity of data.
- Apply ETL transformations, including joins and calculations.
- Output results to S3 for further analysis or processing.

The job is modular, flexible, and easily extendable to handle additional providers or new processing logic with minimal code changes.

## Prerequisites

Before setting up and running the AWS Glue job, ensure you have the following:

- **AWS Account** with permissions to use AWS Glue, S3, and AWS Glue Data Catalog.
- **AWS Glue Data Catalog** tables are set up and accessible (e.g., `payin_psp_report_homo`, `session_psp_report_homo`, `datamart_revenue_internal1`).
- **S3 Bucket** to store the output results.
- **IAM Role** for AWS Glue with the necessary permissions to read from the Glue Data Catalog and S3 and write output data to S3.

## Script Description

The PySpark script (`glue_job_etl.py`) is designed to perform the following:

1. **Load Data**: Uses the `load_data` function to load data from the Glue Data Catalog into Spark DataFrames.
2. **Data Preparation and Transformation**:
   - Filters and transforms data for each service provider based on configuration settings.
   - Joins multiple datasets and calculates derived metrics.
3. **Error Handling**: Incorporates try-except blocks to catch and log errors during data processing.
4. **Data Quality Checks**: Ensures data integrity before and after transformations.
5. **Output Results**: Writes the transformed data to S3 in Parquet format.

## Configuration

The script uses a dictionary called `provider_configurations` to define specific settings for each provider, including date ranges for filtering:

```python
provider_configurations = {
    'checkout_sg': {
        'payin_filter_date_start': '2024-07-31',
        'payin_filter_date_end': '2024-08-21',
        'session_filter_date_start': '2024-07-31',
        'session_filter_date_end': '2024-08-21'
    },
    'xendit': {
        'payin_filter_date_start': '2024-05-31',
        'payin_filter_date_end': '2024-08-14',
        'session_filter_date_start': '2024-07-31',
        'session_filter_date_end': '2024-08-21'
    }
}



Execution
To execute the AWS Glue job, follow these steps:

1. Upload the Script to S3
Save the script (glue_job_etl.py) locally.
Upload the script to an S3 bucket that is accessible by AWS Glue.
2. Create the AWS Glue Job
Navigate to the AWS Glue console.
Click on "Jobs" and then select "Add Job".
Configure the job properties:
Name: Provide a unique name for your job.
IAM Role: Select an IAM role with the necessary permissions to access the Glue Data Catalog and S3.
Script path: Provide the S3 path where the uploaded script is located.
Python version: Choose Python 3.6 or higher.
Worker type and number: Set the number of workers and specify the worker type.
3. Run the Job
Click "Save" and then "Run" to start the job.
Monitor the job execution in the AWS Glue console under "Jobs > Runs".
Error Handling and Logging
The script uses try-except blocks to handle errors at various stages of data processing.
Errors are logged using the Python logging module, which provides detailed information about the error and the operation affected.
Logs can be accessed through Amazon CloudWatch Logs for monitoring and troubleshooting purposes.
Data Quality Checks
Before processing, the script checks for non-empty DataFrames and validates that essential columns are present.
Data quality checks are performed after each major transformation to ensure data integrity.
If any data quality check fails, an error is logged, and the processing for that provider is halted.
Extending the Script
To add support for new providers or modify the processing logic:

Update Configuration:

Add a new entry in the provider_configurations dictionary with the required parameters.
Custom Logic:

If the new provider requires unique processing logic, add the necessary logic within the corresponding functions (e.g., prepare_payin_session_dataframes).
Run the Job:

Execute the AWS Glue job, which will automatically process the new provider based on the updated configuration.