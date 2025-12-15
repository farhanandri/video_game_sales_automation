import datetime as dt
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'farhan-pyspark',
    'start_date': dt.datetime(2025, 11, 23),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}

with DAG('vg_sales_pyspark',
         default_args=default_args,
         schedule_interval='*/5 * * * *',
         catchup=False,
         ) as dag:

    python_extract_pyspark = BashOperator(task_id='vg_sales_extract', bash_command='sudo -u airflow python /opt/airflow/scripts/extract_pyspark.py')
    python_transform_pyspark = BashOperator(task_id='vs_sales_transform', bash_command='sudo -u airflow python /opt/airflow/scripts/transform_pyspark.py')
    python_load_pyspark = BashOperator(task_id='vg_sales_load', bash_command='sudo -u airflow python /opt/airflow/scripts/load_pyspark.py')
    

python_extract_pyspark >> python_transform_pyspark >> python_load_pyspark