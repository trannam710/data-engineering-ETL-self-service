from airflow.models.baseoperator import BaseOperator
from airflow.providers.http.hooks.http import HttpHook
import os

class GenericApiToLocalOperator(BaseOperator):
    # Template fields
    template_fields = ('s3_key', 'api_params')

    def __init__(self, *, http_conn_id, endpoint, local_path, api_params=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.local_path = local_path
        self.api_params = api_params or {}

    def execute(self, context):
        self.log.info(f"Call API endpoint: {self.endpoint} with params: {self.api_params}")

        # 1. Use HttpHook to call API
        http_hook = HttpHook(method='GET', http_conn_id=self.http_conn_id)
        response = http_hook.run(self.endpoint, data=self.api_params)

        # 2. Check response
        response.raise_for_status()

        # 3. Write data to a local file
        self.log.info(f"Writing data to file: {self.local_path}")
        with open(self.local_path, "w") as f:
            f.write(response.text)
        self.log.info("Write data successfully!")

        return self.local_path