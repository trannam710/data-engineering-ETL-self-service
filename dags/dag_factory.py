import yaml
from pathlib import Path
from airflow import DAG
# ... import các operator cần thiết

CONFIGS_DIR = Path(__file__).parent.parent / "configs"

# Lặp qua tất cả các file .yaml trong thư mục configs
for config_file in CONFIGS_DIR.glob("*.yaml"):
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)

    # Tạo một DAG object từ config
    dag_id = f"ingest_{config['pipeline_name']}"
    
    with DAG(
        dag_id=dag_id,
        schedule_interval=config['schedule'],
        default_args={'owner': config['owner']},
        # ... các tham số khác
    ) as dag:
        # Tạo các task từ config
        ingest_task = GenericApiToS3Operator(
            task_id="ingest_from_api",
            api_conn_id=config['source']['connection_id'],
            # ...
        )
        
        validate_task = DataQualityCheckOperator(...)

        # ... các task khác

    # Đưa DAG vừa tạo vào global scope để Airflow nhận diện
    globals()[dag_id] = dag