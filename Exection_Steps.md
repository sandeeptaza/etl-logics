## Execution

To execute the AWS Glue job, follow these steps:

### 1. Upload the Script to S3

- Save the script (`glue_job_etl.py`) locally.
- Upload the script to an S3 bucket that is accessible by AWS Glue.

### 2. Create the AWS Glue Job

- Navigate to the AWS Glue console.
- Click on **"Jobs"** and then select **"Add Job"**.
- Configure the job properties:
  - **Name**: Provide a unique name for your job.
  - **IAM Role**: Select an IAM role with the necessary permissions to access the Glue Data Catalog and S3.
  - **Script path**: Provide the S3 path where the uploaded script is located.
  - **Python version**: Choose Python 3.6 or higher.
  - **Worker type and number**: Set the number of workers and specify the worker type.

### 3. Run the Job

- Click **"Save"** and then **"Run"** to start the job.
- Monitor the job execution in the AWS Glue console under **"Jobs > Runs"**.

## Error Handling and Logging

- The script uses `try-except` blocks to handle errors at various stages of data processing.
- Errors are logged using the Python `logging` module, which provides detailed information about the error and the operation affected.
- Logs can be accessed through **Amazon CloudWatch Logs** for monitoring and troubleshooting purposes.

## Data Quality Checks

- Before processing, the script checks for non-empty DataFrames and validates that essential columns are present.
- Data quality checks are performed after each major transformation to ensure data integrity.
- If any data quality check fails, an error is logged, and the processing for that provider is halted.

## Extending the Script

To add support for new providers or modify the processing logic:

1. **Update Configuration**:
   - Add a new entry in the `provider_configurations` dictionary with the required parameters.

2. **Custom Logic**:
   - If the new provider requires unique processing logic, add the necessary logic within the corresponding functions (e.g., `prepare_payin_session_dataframes`).

3. **Run the Job**:
   - Execute the AWS Glue job, which will automatically process the new provider based on the updated configuration.


------------------------------------------


# Running AWS Glue Job with Dynamic Parameters in the AWS Glue Console

This guide outlines how to run an AWS Glue job with dynamic parameters directly from the **AWS Glue Console (UI)**.



## Step 1: Create or Update a Glue Job
- Navigate to the AWS Glue Console and create a new Glue job, or select an existing job to update.


## Step 2: Open the Glue Job in the Console
- In the **AWS Glue Console**, click on the job you want to edit or run.

## Step 3: Configure Job Parameters
- In the **"Script arguments"** section of the job configuration, click **"Add new parameter"**.

## Step 4: Add Dynamic Parameters
- Add each of the required parameters defined in your script. For example:
  
| Key                       | Value           |
|---------------------------|-----------------|
| `--PROVIDER`               | `checkout_sg`   |
| `--PAYIN_FILTER_DATE_START`| `2024-07-31`    |
| `--PAYIN_FILTER_DATE_END`  | `2024-08-21`    |
| `--SESSION_FILTER_DATE_START`| `2024-07-31`  |
| `--SESSION_FILTER_DATE_END`| `2024-08-21`    |

## Step 5: Save the Job
- After adding the parameters, click **"Save"** to save the job configuration.

## Step 6: Run the Glue Job
- In the AWS Glue Console, click **"Run Job"** to execute the job with the dynamic parameters.
- The job will now use the provided arguments during execution.

## Step 7: Verify Job Execution
- Monitor the job's execution in
 **AWS CloudWatch Logs** to ensure the parameters are being passed correctly.
- Verify the job output and logs to confirm successful execution.

