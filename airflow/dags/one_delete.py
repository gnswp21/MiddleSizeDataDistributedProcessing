from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from callables import *


default_args = {
    'owner': 'airflow',
}
cluster_names = ['mid-cluster-1']

with DAG(dag_id='delete_cluster_one',
         description='delete_cluster_dag',
         default_args=default_args,
         schedule_interval=None,
         params={'tuning-id':1},
         catchup=False) as dag:

    for cluster_name in cluster_names:
        with TaskGroup(group_id=f'cluster_{cluster_name}') as cluster_group:
            get_emr_virtual_cluster_id = PythonOperator(
                task_id='get_emr_virtual_cluster_id',
                python_callable=get_emr_virtual_cluster_id_by_bash,
                op_kwargs={'cluster_name': cluster_name}
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
