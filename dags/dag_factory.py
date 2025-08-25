# dags/dag_factory.py
import yaml
from pathlib import Path
from airflow.models.dag import DAG
from datetime import datetime

from operators.api_to_s3 import GenericApiToS3Operator
from operators.data_quality_operator import DataQualityCheckOperator

# Import operators
from airflow.operators.python import BranchPythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator


# Path to the directory containing configuration files
CONFIG_DIR = Path(__file__).resolve().parent.parent / "configs/sources"

# Thêm dòng này để kiểm tra xem file đã được thực thi chưa
print(f"Airflow is processing DAG factory from: {__file__}")
print(f"Searching for YAML files in directory: {CONFIG_DIR}")

def create_dag(config: dict):

    """
    This function takes a configuration dictionary and returns a DAG object.
    """
    pipeline_info = config['pipeline_info']
    source_config = config['source']
    dest_config = config['destination']
    dq_checks_config = config['data_quality_checks']

    dag_id = f"dynamic_ingest_{pipeline_info['name']}"

    # --- Branching ---
    def check_dq_result(ti):
        """Read data quality check results from XComs and decide the branch to take."""
        dq_result = ti.xcom_pull(task_ids='run_data_quality_checks', key='dq_summary')
        if dq_result['status'] == 'PASSED':
            return 'all_checks_passed' # Task ID of the success branch
        return 'some_checks_failed' # Task ID of the failure branch

    # --- DAG ---
    with DAG(
        dag_id=dag_id,
        start_date=datetime(2024, 1, 1),
        schedule_interval=pipeline_info['schedule'],
        tags=pipeline_info.get('tags', []),
        doc_md=pipeline_info.get('description', ''),
        catchup=False
    ) as dag:
        
        # Task 1: Get data from API and save to S3
        ingest_task = GenericApiToS3Operator(
            task_id="ingest_from_api",
            http_conn_id=source_config['connection_id'],
            endpoint=source_config['endpoint'],
            s3_conn_id=dest_config['connection_id'],
            s3_bucket=dest_config['bucket'],
            s3_key=dest_config['path'], # S3 path
            api_params=source_config.get('params', {})
        )

        # Task 2: Data Quality Check
        dq_check_task = DataQualityCheckOperator(
            task_id="run_data_quality_checks",
            s3_conn_id=dest_config['connection_id'],
            s3_bucket=dest_config['bucket'],
            # Get key of S3 file from previous task via XComs
            s3_key=ingest_task.output,
            checks=dq_checks_config
        )

        # Task 3: Branching logic based on data quality check results
        branch_task = BranchPythonOperator(
            task_id='check_dq_result_branch',
            python_callable=check_dq_result,
        )

        # Send a Slack notification if data quality checks fail
        fail_task = SlackWebhookOperator(
            task_id='some_checks_failed',
            slack_webhook_conn_id='slack_conn',
            message="""
            :red_circle: DAG {{ dag.dag_id }} fail in quality check.
            Reason: {{ ti.xcom_pull(task_ids='run_data_quality_checks', key='dq_summary')['errors'] }}
            """,
        )

        # Success task can be a placeholder or another operator
        from airflow.operators.empty import EmptyOperator
        success_task = EmptyOperator(task_id='all_checks_passed')
        
        # Connect tasks
        ingest_task >> dq_check_task >> branch_task
        branch_task >> [success_task, fail_task]

    return dag

# Loop through all YAML config files in the configs directory
for config_file in CONFIG_DIR.glob("*.yaml"):
    print(f"Processing config file: {config_file}")
    # Load the YAML file
    with open(config_file, "r") as f:
        config_data = yaml.safe_load(f)
        dag_object = create_dag(config_data)
        globals()[dag_object.dag_id] = dag_object