from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

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

    # Check EMR Virtual Cluster Id
    # check_emr_virtual_cluster_id =  BashOperator(
    #     task_id='check_emr_virtual_cluster_id',  # task_id 수정 (공백 제거)
    #     bash_command="aws emr-containers list-virtual-clusters"
    #                  "--region ap-northeast-2"
    #                  "--query 'virtualClusters[?name==`mid_emr_virtual_cluster` && state==`RUNNING`].id'",
    #     xcom_push=True,
    #
    #
    # )


    # Run EMR on EKS Job
    run_job = BashOperator(
        task_id='run_job',  # task_id 수정 (공백 제거)
        bash_command='aws emr-containers start-job-run --cli-input-json file:///etl/process/job-run.json'
    )


