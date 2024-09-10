import subprocess
from callable import *
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}

with DAG(dag_id='delete_cluster',
         description='delete_cluster_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    get_emr_virtual_cluster_id = PythonOperator(
        task_id='get_emr_virtual_cluster_id',
        python_callable=get_emr_virtual_cluster_id_by_bash,
    )
    # Run EMR on EKS Job
    delete_emr_virtual_cluster = PythonOperator(
        task_id='delete_emr_virtual_cluster',
        python_callable=delete_emr_virtual_cluster_func,
        provide_context=True
    )

    # Delete EMR Virtual Cluster
    delete_eks_cluster = BashOperator(
        task_id='delete_eks_cluster',
        bash_command='eksctl delete cluster --name mid-cluster'
    )

    get_emr_virtual_cluster_id >> delete_emr_virtual_cluster >> delete_eks_cluster
