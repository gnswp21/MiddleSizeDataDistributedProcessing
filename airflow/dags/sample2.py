from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}

with DAG(dag_id='bash', description='check_user_dag',default_args=default_args,schedule_interval='0 0 * * *',catchup=False) as dag:
    check_user = BashOperator(
        task_id='check_aws',
        bash_command='aws s3 ls'
    )


