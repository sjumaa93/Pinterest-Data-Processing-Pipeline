from pydoc import describe
from airflow.models import DAG
from datetime import datetime
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from s3_to_spark import run_s3_to_spark

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sjumaa93@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2022, 9, 12),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 9, 13),
}


dag = DAG(
        dag_id='pinterest_dag',
        default_args=default_args,
        catchup=False,
        schedule_interval=timedelta(days=1)
        )


run_task = PythonOperator(
    task_id='pinterest_total',
    python_callable=run_s3_to_spark,
    dag=dag,
)

run_task