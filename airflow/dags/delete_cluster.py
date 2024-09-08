from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}

with DAG(dag_id='delete_cluster',
         description='delete_cluster_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    # Delete EMR Virtual Cluster
    delete_emr_virtual_cluster = BashOperator(
        task_id='delete_emr_virtual_cluster',
        bash_command='aws emr-containers delete-virtual-cluster --id 9npapnc9scs710z1wabnvd6tq'
    )

    # Delete EMR Virtual Cluster
    delete_eks_cluster = BashOperator(
        task_id='delete_eks_cluster',
        bash_command='eksctl delete cluster --name mid-cluster'
    )

    delete_emr_virtual_cluster >> delete_eks_cluster

