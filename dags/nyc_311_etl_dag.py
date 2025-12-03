from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# loading task modules
from scripts.get_nyc311_data import get_nyc311_data
from scripts.flatten_json import flatten_json_file
from scripts.transformations import transformations
from scripts.save_to_s3 import save_to_s3

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@daily',
}

dag_nyc311 = DAG(
    'nyc_311_data_pipeline',
    default_args=default_args,
    description='A DAG to fetch and process NYC 311 data',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)


fetch_data = PythonOperator(
    task_id='fetch_nyc311_data',
    python_callable=get_nyc311_data,
    dag=dag_nyc311,
)

flatten_data = PythonOperator(
    task_id='flatten_nyc311_data',
    python_callable=flatten_json_file,
    dag=dag_nyc311,
)

transform_data = PythonOperator(
    task_id='transform_nyc311_data',
    python_callable=transformations,
    dag=dag_nyc311,
)

upload_to_s3 = PythonOperator(
    task_id='upload_nyc311_data_to_s3',
    python_callable=save_to_s3,
    dag=dag_nyc311,
    op_kwargs={
        "bucket_name": "nyc311-airflow-data-bucket",
        "s3_key": "/cleaned/nyc_311_cleaned_first_1m."
    },
)

fetch_data >> flatten_data >> transform_data >> upload_to_s3

