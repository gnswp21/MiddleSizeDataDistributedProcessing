import subprocess

from airflow.utils.task_group import TaskGroup

from callable import *
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}
cluster_names = ['mid-cluster-1', 'mid-cluster-2', 'mid-cluster-3']
with DAG(dag_id='delete_cluster_multi',
         description='delete_cluster_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    for cluster_name in cluster_names:
        with TaskGroup(group_id=f'cluster_{cluster_name}') as cluster_group:
            get_emr_virtual_cluster_id = PythonOperator(
                task_id='get_emr_virtual_cluster_id',
                python_callable=get_emr_virtual_cluster_id_by_bash,
                op_kwargs={'cluster_name':cluster_name}
            )
            # Run EMR on EKS Job
            delete_emr_virtual_cluster = PythonOperator(
                task_id='delete_emr_virtual_cluster',
                python_callable=delete_emr_virtual_cluster_func,
                op_kwargs={'cluster_name': cluster_name},
                provide_context=True
            )

            # Delete EMR Virtual Cluster (aws cli 기반)
            delete_eks_cluster = BashOperator(
                task_id='delete_eks_cluster',
                bash_command=f'eksctl delete cluster --name {cluster_name} '
            )

            get_emr_virtual_cluster_id >>  delete_emr_virtual_cluster >> delete_eks_cluster
