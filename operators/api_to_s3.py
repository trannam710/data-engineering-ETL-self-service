from __future__ import annotations

import logging
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Create a logger
log = logging.getLogger(__name__)

class GenericApiToS3Operator(BaseOperator):
    """
    Fetches data from a generic API endpoint and uploads the response
    to a specified S3 bucket.

    Args:
        http_conn_id (str): The HTTP connection ID to use for the API call.
        endpoint (str): The API endpoint to call.
        s3_conn_id (str): The S3 connection ID to use for S3 operations.
        s3_bucket (str): The name of the S3 bucket to upload the data to.
        s3_key (str): The key (path) within the S3 bucket to save the data.
        api_params (dict | None): A dictionary of parameters to pass to the API.
    """

    # We need to template the s3_key and api_params as they often contain
    # Airflow macros like "{{ ds }}".
    template_fields = ('s3_key', 'api_params')

    def __init__(
        self, 
        *, 
        http_conn_id: str, 
        endpoint: str, 
        s3_conn_id: str,
        s3_bucket: str,
        s3_key: str,
        api_params: dict[str, Any] | None = None, 
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.api_params = api_params or {}

    def execute(self, context) -> str:
        """
        Main execution logic for the operator.
        """
        self.log.info(f"Calling API endpoint: {self.endpoint} with params: {self.api_params}")

        # 1. Use HttpHook to call the API
        http_hook = HttpHook(method='GET', http_conn_id=self.http_conn_id)
        response = http_hook.run(self.endpoint, data=self.api_params)

        # 2. Check the API response status
        response.raise_for_status()
        data = response.text

        # 3. Use S3Hook to upload data to S3
        self.log.info(f"Uploading data to S3 at: s3://{self.s3_bucket}/{self.s3_key}")
        
        s3_hook = S3Hook(aws_conn_id=self.s3_conn_id)
        s3_hook.load_string(
            string_data=data,
            key=self.s3_key,
            bucket_name=self.s3_bucket,
            replace=True,  # Overwrite the file if it already exists
        )
        self.log.info("Data uploaded successfully to S3!")

        # Push the S3 key to XComs so the next task can use it
        return self.s3_key