from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from callables import *

default_args = {
    'owner': 'airflow',
}

cluster_names = ['mid-cluster-1']
nodes =['2']
ports = ['19090']

with DAG(dag_id='run_job_one',
         description='run_job_dag',
         default_args=default_args,
         schedule_interval=None,
         params={'tuning-id':1},
         catchup=False) as dag:

    def create_job_operators(cluster_name, port, job_id):
        from callables import save_job_result
        run_job = PythonOperator(
            task_id=f'run_job_{job_id}',
            python_callable=run_job_func,
            op_kwargs={'id': job_id, 'cluster_name': cluster_name},
            provide_context=True
        )
        wait_job = ShortCircuitOperator(
            task_id=f'wait_job_{job_id}',
            python_callable=wait_job_done,
            op_kwargs={'id': job_id, 'cluster_name': cluster_name},
            provide_context=True
        )
        save_job_result = PythonOperator(
            task_id=f'save_job_result_{job_id}',
            python_callable=save_job_result,
            op_kwargs={'id': job_id, 'cluster_name': cluster_name, 'port': port},
            provide_context=True
        )
        return run_job, wait_job, save_job_result

    for i, (cluster_name, port) in enumerate(zip(cluster_names, ports)):
        with TaskGroup(group_id=f'cluster_{cluster_name}') as cluster_group:

            get_emr_virtual_cluster_id = PythonOperator(
                task_id='get_emr_virtual_cluster_id',
                python_callable=get_emr_virtual_cluster_id_by_bash,
                op_kwargs={'cluster_name': cluster_name}
            )

            get_eks_arn = BashOperator(
                task_id='get_eks_arn',
                bash_command=f'aws eks describe-cluster --name {cluster_name} --query "cluster.arn" --output text',
                do_xcom_push=True
            )

            get_kubeconfig = BashOperator(
                task_id='get_kubeconfig',
                bash_command=f"aws eks update-kubeconfig --name {cluster_name} --kubeconfig /tmp/{cluster_name}_config"
            )

            port_forward = BashOperator(
                task_id='port_forward',
                bash_command=f"kubectl --kubeconfig /tmp/{cluster_name}_config port-forward prometheus-monitoring-{cluster_name}-k-prometheus-0 {port}:9090 &>log &"
            )

            port_forward_stop = BashOperator(
                task_id='port_forward_stop',
                bash_command=f"kill -9 $(lsof -t -i :{port})"
            )

            # 반복적인 작업들을 함수로 처리
            run_job_1, wait_job_1, save_job_result_1 = create_job_operators(cluster_name, port, '1')


            # 태스크 연결
            get_emr_virtual_cluster_id >> get_eks_arn >> get_kubeconfig >> port_forward >> \
            run_job_1 >> wait_job_1 >> save_job_result_1 >> \
            port_forward_stop
