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
                op_kwargs={'cluster_name':cluster_name},
                provide_context=True
            )

            # Update k8s config
            update_k8s_config = BashOperator(
                task_id='update_k8s_config',
                bash_command=f"aws eks update-kubeconfig --name {cluster_name} --kubeconfig ./{cluster_name}_config"
            )


            # Delete EMR Virtual Cluster
            delete_eks_cluster = BashOperator(
                task_id='delete_eks_cluster',
                bash_command=f'eksctl delete cluster --name {cluster_name} --kubeconfig ./{cluster_name}_config'
            )

            get_emr_virtual_cluster_id >> update_k8s_config >> delete_emr_virtual_cluster >> delete_eks_cluster