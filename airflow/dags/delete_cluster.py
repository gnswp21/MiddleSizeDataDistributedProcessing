import subprocess

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}


def get_emr_virtual_cluster_id_by_bash():
    args = "aws emr-containers list-virtual-clusters --region ap-northeast-2 --query".split()
    args.append('virtualClusters[?name==`mid_emr_virtual_cluster` && state==`RUNNING`].id')
    print(args)
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        case = result.stdout.strip()
        virtual_cluster_id = case[1:-1].strip()[1:-1]
        return {'virtual_cluster_id': virtual_cluster_id}


def delete_emr_virtual_cluster_func(**kwargs):
    # XCom에서 값을 가져옴
    ti = kwargs['ti']
    virtual_cluster_id = ti.xcom_pull(task_ids='get_emr_virtual_cluster_id', key='return_value')['virtual_cluster_id']
    args = 'aws emr-containers delete-virtual-cluster --id'.split()
    args.extend([virtual_cluster_id])
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)


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
        task_id='delete_emr_virtual_cluster',  # task_id 수정 (공백 제거)
        python_callable=delete_emr_virtual_cluster_func,
        provide_context=True
    )

    # Delete EMR Virtual Cluster
    delete_eks_cluster = BashOperator(
        task_id='delete_eks_cluster',
        bash_command='eksctl delete cluster --name mid-cluster'
    )

    get_emr_virtual_cluster_id >> delete_emr_virtual_cluster >> delete_eks_cluster
