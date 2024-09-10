import subprocess
from callable import *
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from airflow.operators.python import PythonOperator, ShortCircuitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}

with DAG(dag_id='run_job',
         description='run_job_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    # TODO Task : check repo is available
    # aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 691487686124.dkr.ecr.ap-northeast-2.amazonaws.com
    # aws ecr describe-images --repository-name emr6.5_mid_repo --region ap-northeast-2

    get_emr_virtual_cluster_id = PythonOperator(
        task_id='get_emr_virtual_cluster_id',
        python_callable=get_emr_virtual_cluster_id_by_bash,
    )

    # Run EMR on EKS Job
    run_job_1 = PythonOperator(
        task_id='run_job_1',
        python_callable=run_job_func,
        op_kwargs={'job_run_id': '1'},
        provide_context=True
    )

    wait_job_1 = ShortCircuitOperator(
        task_id='wait_job_1',
        python_callable=wait_job_done,
        op_kwargs={'task_ids': 'run_job_1'},
        provide_context=True
    )

    # Run EMR on EKS Job
    run_job_2 = PythonOperator(
        task_id='run_job_2',
        python_callable=run_job_func,
        op_kwargs={'job_run_id': '2'},
        provide_context=True
    )

    wait_job_2 = ShortCircuitOperator(
        task_id='wait_job_2',
        python_callable=wait_job_done,
        op_kwargs={'task_ids': 'run_job_2'},
        provide_context=True
    )

    # Run EMR on EKS Job
    run_job_3 = PythonOperator(
        task_id='run_job_3',
        python_callable=run_job_func,
        op_kwargs={'job_run_id': '3'},
        provide_context=True
    )

    wait_job_3 = ShortCircuitOperator(
        task_id='wait_job_3',
        python_callable=wait_job_done,
        op_kwargs={'task_ids': 'run_job_3'},
        provide_context=True
    )

    # Run EMR on EKS Job
    run_job_4 = PythonOperator(
        task_id='run_job_4',
        python_callable=run_job_func,
        op_kwargs={'job_run_id': '4'},
        provide_context=True
    )

    wait_job_4 = ShortCircuitOperator(
        task_id='wait_job_4',
        python_callable=wait_job_done,
        op_kwargs={'task_ids': 'run_job_4'},
        provide_context=True
    )

    get_emr_virtual_cluster_id >> run_job_1 >> wait_job_1 >> \
    run_job_2 >> wait_job_2 >> \
    run_job_3 >> wait_job_3 >> \
    run_job_4 >> wait_job_4
