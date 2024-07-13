import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionCreatePipelineOperator

default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'email': ['divyanshsankhla2000@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
dag = DAG(
    'employee_data',
    default_args=default_args,
    description='Employee data files',
    schedule_interval='*/10 * * * *',
    max_active_runs=2,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
)
with dag:
    process_task = BashOperator(
    task_id='extract_employee_data',
    bash_command='python /home/airflow/gcs/dags/EmployeeDataScripts/extract.py',
    )

    create_pipeline = CloudDataFusionCreatePipelineOperator(
    location='asia-east1',
    pipeline_name='etl_data_pipeline',
    instance_name='datafusion-divyansh',
    task_id="start_datafusion_pipeline",
    )

    process_task >> create_pipeline
