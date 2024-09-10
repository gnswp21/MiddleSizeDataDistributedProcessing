import subprocess

from airflow.utils.task_group import TaskGroup

from callable import *
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

from airflow.operators.python import PythonOperator, ShortCircuitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}

cluster_names = ['mid-cluster-1', 'mid-cluster-2', 'mid-cluster-3']
ports = {'mid-cluster-1':19090, 'mid-cluster-2':29090, 'mid-cluster-3':39090}

with DAG(dag_id='run_job_multi',
         description='run_job_dag',
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

            get_eks_arn = BashOperator(
                task_id='get_eks_arn',
                bash_command=f'aws eks describe-cluster --name {cluster_name} --query "cluster.arn" --output text',
                do_xcom_push=True

            )

            # 포트 포워딩 시작 (데몬으로 실행)
            port_forward = PythonOperator(
                task_id='port_forward',
                python_callable=set_port_forwarding,
                provide_context=True,
                op_kwargs={'cluster_name':cluster_name, 'port':ports[cluster_name]}
            )

            # Run EMR on EKS Job
            # run_job_1 = PythonOperator(
            #     task_id='run_job_1',
            #     python_callable=run_job_func,
            #     op_kwargs={'id': '1', 'cluster_name':cluster_name},
            #     provide_context=True
            # )
            #
            # wait_job_1 = ShortCircuitOperator(
            #     task_id='wait_job_1',
            #     python_callable=wait_job_done,
            #     op_kwargs={'id': '1', 'cluster_name':cluster_name},
            #     provide_context=True
            # )
            #
            # save_job_result_1 = PythonOperator(
            #     task_id='save_job_result_1',
            #     python_callable=save_job_result,
            #     op_kwargs={'id': '1', 'cluster_name':cluster_name, 'port':ports[cluster_name]},
            #     provide_context=True
            # )
            # #
            # # 포트 포워딩 종료
            # port_forward_stop = BashOperator(
            #     task_id='port_forward_stop',
            #     bash_command=f"pkill -f 'kubectl port-forward prometheus-monitoring-{cluster_name}-k-prometheus-0'"
            # )

            # # Run EMR on EKS Job
            # run_job_2 = PythonOperator(
            #     task_id='run_job_2',
            #     python_callable=run_job_func,
            #     op_kwargs={'id': '2'},
            #     provide_context=True
            # )
            #
            # wait_job_2 = ShortCircuitOperator(
            #     task_id='wait_job_2',
            #     python_callable=wait_job_done,
            #     op_kwargs={'id': '2'},
            #     provide_context=True
            # )
            #
            # save_job_result_2 = PythonOperator(
            #     task_id='save_job_result_2',
            #     python_callable=save_job_result,
            #     op_kwargs={'id': '2'},
            #     provide_context=True
            # )
            #
            # # Run EMR on EKS Job
            # run_job_3 = PythonOperator(
            #     task_id='run_job_3',
            #     python_callable=run_job_func,
            #     op_kwargs={'id': '3'},
            #     provide_context=True
            # )
            #
            # wait_job_3 = ShortCircuitOperator(
            #     task_id='wait_job_3',
            #     python_callable=wait_job_done,
            #     op_kwargs={'id': '3'},
            #     provide_context=True
            # )
            #
            # save_job_result_3 = PythonOperator(
            #     task_id='save_job_result_3',
            #     python_callable=save_job_result,
            #     op_kwargs={'id': '3'},
            #     provide_context=True
            # )
            #
            # # Run EMR on EKS Job
            # run_job_4 = PythonOperator(
            #     task_id='run_job_4',
            #     python_callable=run_job_func,
            #     op_kwargs={'id': '4'},
            #     provide_context=True
            # )
            #
            # wait_job_4 = ShortCircuitOperator(
            #     task_id='wait_job_4',
            #     python_callable=wait_job_done,
            #     op_kwargs={'id': '4'},
            #     provide_context=True
            # )
            #
            # save_job_result_4 = PythonOperator(
            #     task_id='save_job_result_4',
            #     python_callable=save_job_result,
            #     op_kwargs={'id': '4'},
            #     provide_context=True
            # )
            get_emr_virtual_cluster_id >> get_eks_arn >> port_forward #>> \
            # run_job_1 >> wait_job_1 >> save_job_result_1 >> \
            # port_forward_stop
    # run_job_2 >> wait_job_2 >> \
    # run_job_3 >> wait_job_3 >> \
    # run_job_4 >> wait_job_4
