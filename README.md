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