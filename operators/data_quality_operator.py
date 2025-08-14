from __future__ import annotations

import logging
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
from io import BytesIO

# Create a logger
log = logging.getLogger(__name__)

class DataQualityCheckOperator(BaseOperator):
    """
    Checks the quality of a file in S3 based on a provided configuration.

    This operator retrieves a JSON file from S3 and performs a series of checks
    as defined in the `checks` parameter. If any check fails, the operator
    will raise an exception.

    Args:
        s3_conn_id (str): The S3 connection ID.
        s3_bucket (str): The name of the S3 bucket.
        s3_key (str): The key (path) to the JSON file in the S3 bucket.
        checks (list[dict]): A list of dictionaries, where each dictionary
                             defines a data quality check.
    """

    # We need to template the s3_key, as it contains Airflow macros.
    template_fields = ["s3_key"]

    def __init__(
        self,
        s3_conn_id: str,
        s3_bucket: str,
        s3_key: str,
        checks: list[dict[str, Any]],
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.checks = checks

    def execute(self, context):
        """
        Main execution logic for the operator.
        """
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        
        # Check if the file exists
        if not s3_hook.check_for_key(self.s3_key, self.s3_bucket):
            raise FileNotFoundError(
                f"File not found in S3 at '{self.s3_bucket}/{self.s3_key}'"
            )

        log.info(f"Starting data quality checks for file: s3://{self.s3_bucket}/{self.s3_key}")
        
        # Read the file from S3 into a Pandas DataFrame
        try:
            s3_obj = s3_hook.get_key(key=self.s3_key, bucket_name=self.s3_bucket)
            if s3_obj is None:
                raise FileNotFoundError("S3 object not found")
            
            file_content = s3_obj.get()['Body'].read()
            # Assuming the file is a JSON file as per your example
            df = pd.read_json(BytesIO(file_content))
            log.info(f"Successfully read data from S3. DataFrame shape: {df.shape}")
        except Exception as e:
            raise RuntimeError(f"Failed to read data from S3 and load into DataFrame: {e}")

        # --- Perform Data Quality Checks based on YAML configuration ---
        failed_checks = []

        for check in self.checks:
            check_type = check.get("check_type")
            
            # 1. Check for minimum row count
            if check_type == "min_row_count":
                threshold = check.get("threshold")
                row_count = len(df)
                if row_count < threshold:
                    failed_checks.append(f"Min row count check failed. Expected at least {threshold} rows, but found {row_count}.")
                    log.error(failed_checks[-1])
                else:
                    log.info(f"Min row count check passed. Found {row_count} rows.")
            
            # 2. Check for required columns
            elif check_type == "required_columns":
                required_cols = set(check.get("columns", []))
                present_cols = set(df.columns)
                missing_cols = required_cols - present_cols
                
                if missing_cols:
                    failed_checks.append(f"Required columns check failed. Missing columns: {', '.join(missing_cols)}")
                    log.error(failed_checks[-1])
                else:
                    log.info("Required columns check passed.")
            
            # 3. Check for unique column values
            elif check_type == "unique_column":
                column_name = check.get("column")
                if column_name in df.columns:
                    is_unique = df[column_name].is_unique
                    if not is_unique:
                        failed_checks.append(f"Unique column check failed for column '{column_name}'. Values are not unique.")
                        log.error(failed_checks[-1])
                    else:
                        log.info(f"Unique column check for '{column_name}' passed.")
                else:
                    failed_checks.append(f"Unique column check failed. Column '{column_name}' not found in data.")
                    log.error(failed_checks[-1])
            
            else:
                log.warning(f"Unknown check type: {check_type}. Skipping.")

        # --- Final result handling ---
        if failed_checks:
            summary_status = "FAILED"
            raise ValueError(f"Data quality checks failed: {'; '.join(failed_checks)}")
        else:
            summary_status = "PASSED"
            log.info("All data quality checks passed successfully!")

        # Push the summary result to XCom
        # This is for the BranchPythonOperator to use
        context['ti'].xcom_push(key='dq_summary', value={'status': summary_status})