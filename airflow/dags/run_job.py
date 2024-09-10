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

    get_emr_virtual_cluster_id = PythonOperator(
        task_id='get_emr_virtual_cluster_id',
        python_callable=get_emr_virtual_cluster_id_by_bash,
    )

    # 포트 포워딩 시작 (데몬으로 실행)
    port_forward_start = BashOperator(
        task_id='port_forward_start',
        bash_command="aws eks update-kubeconfig --name mid-cluster && "
                     "kubectl port-forward prometheus-monitoring-kube-prometheus-prometheus-0 9090:9090 &>/dev/null &"
                     # "kubectl port-forward prometheus-monitoring-kube-prometheus-prometheus-0 9090:9090"
    )

    # # Run EMR on EKS Job
    # run_job_1 = PythonOperator(
    #     task_id='run_job_1',
    #     python_callable=run_job_func,
    #     op_kwargs={'id': '1'},
    #     provide_context=True
    # )
    #
    # wait_job_1 = ShortCircuitOperator(
    #     task_id='wait_job_1',
    #     python_callable=wait_job_done,
    #     op_kwargs={'id': '1'},
    #     provide_context=True
    # )
    #
    # save_job_result_1 = PythonOperator(
    #     task_id='save_job_result_1',
    #     python_callable=save_job_result,
    #     op_kwargs={'id': '1'},
    #     provide_context=True
    # )
    #
    # # 포트 포워딩 종료
    # port_forward_stop = BashOperator(
    #     task_id='port_forward_stop',
    #     bash_command="pkill -f 'kubectl port-forward prometheus-monitoring-kube-prometheus-prometheus-0'"
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
    get_emr_virtual_cluster_id >> port_forward_start
    # get_emr_virtual_cluster_id >> port_forward_start >> \
    # run_job_1 >> wait_job_1 >> save_job_result_1 >> \
    # port_forward_stop
    # run_job_2 >> wait_job_2 >> \
    # run_job_3 >> wait_job_3 >> \
    # run_job_4 >> wait_job_4
