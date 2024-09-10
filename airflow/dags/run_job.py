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


def run_job_func(dataset, task_id, **kwargs):
    # XCom에서 값을 가져옴
    ti = kwargs['ti']
    virtual_cluster_id = ti.xcom_pull(task_ids='get_emr_virtual_cluster_id', key='return_value')['virtual_cluster_id']
    args = 'aws emr-containers start-job-run --cli-input-json file:///opt/airflow/config/job-run.json'.split()
    args.extend(["--virtual-cluster-id", virtual_cluster_id])
    entryPointArguments = ["--job-driver",
                           "'" + '{"sparkSubmitJobDriver": {"entryPointArguments": ["dev-job-1", "%s", "%s"]}}' % (dataset, task_id) + "'"]
    args.extend(entryPointArguments)
    print(args)
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)


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
        task_id='run_job_1',  # task_id 수정 (공백 제거)
        python_callable=run_job_func,
        op_kwargs={'entry_point_args': '["dev-dataset", "task1"]'},
        provide_context=True
    )

    # Run EMR on EKS Job
    run_job_2 = PythonOperator(
        task_id='run_job_2',  # task_id 수정 (공백 제거)
        python_callable=run_job_func,
        op_kwargs={'entry_point_args': '["dev-dataset", "task1"]'},
        provide_context=True
    )

    # Run EMR on EKS Job
    run_job_3 = PythonOperator(
        task_id='run_job_3',  # task_id 수정 (공백 제거)
        python_callable=run_job_func,
        op_kwargs={'entry_point_args': '["dev-dataset", "task1"]'},
        provide_context=True
    )

    # Run EMR on EKS Job
    run_job_4 = PythonOperator(
        task_id='run_job_4',  # task_id 수정 (공백 제거)
        python_callable=run_job_func,
        op_kwargs={'entry_point_args': '["dev-dataset", "task1"]'},
        provide_context=True
    )

    get_emr_virtual_cluster_id >> run_job_1 >> run_job_2 >> run_job_3 >> run_job_4
