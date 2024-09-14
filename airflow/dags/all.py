from datetime import datetime
from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from callables import *

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}

cluster_names = ['mid-cluster-1', 'mid-cluster-2', 'mid-cluster-3']
nodes = ['2', '4', '8']
ports = ['19090', '29090', '39090']

with DAG(dag_id='create_run_delete_all',
         description='create_multiple_clusters_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    for i in range(3):
        cluster_name = cluster_names[i]
        node = nodes[i]
        port = ports[i]
        with TaskGroup(group_id=f'cluster_{cluster_name}') as cluster_group:
            # Create EKS Cluster
            create_eks_cluster = BashOperator(
                task_id=f'create_eks_cluster_{cluster_name}',
                bash_command=f'eksctl create cluster --name {cluster_name} --with-oidc --instance-types=m5.xlarge'
                             f' --managed --nodes={node}'
            )

            # Create IAM Identity Mapping
            create_iamidentitymapping = BashOperator(
                task_id=f'create_iamidentitymapping_{cluster_name}',
                bash_command=f'eksctl create iamidentitymapping --cluster {cluster_name}'
                             f' --service-name "emr-containers" '
                             f' --namespace default'
            )

            # Update Trust Policy
            update_trust_policy = BashOperator(
                task_id=f'update_trust_policy_{cluster_name}',
                bash_command=f'aws emr-containers update-role-trust-policy --cluster-name {cluster_name} '
                             '--namespace default '
                             '--role-name EMRContainers-JobExecutionRole'
            )

            # Create EMR Virtual Cluster
            create_emr_virtual_cluster = BashOperator(
                task_id=f'create_emr_virtual_cluster_{cluster_name}',
                bash_command=f"""
                    aws emr-containers create-virtual-cluster \
                    --name {cluster_name}_emr_virtual_cluster \
                    --container-provider '{{
                        "id": "{cluster_name}",
                        "type": "EKS",
                        "info": {{"eksInfo": {{"namespace": "default"}}}}
                    }}'
                """
            )

            # Add kube Prometheus stack by helm
            add_kube_prometheus_stack = BashOperator(
                task_id=f'add_kube_prometheus_stack_{cluster_name}',
                bash_command=f"""
                aws eks update-kubeconfig --name {cluster_name} --kubeconfig ./{cluster_name}_config
                helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
                helm repo update
                helm install monitoring-{cluster_name} prometheus-community/kube-prometheus-stack --kubeconfig ./{cluster_name}_config
                """
            )


            def create_job_operators(cluster_name, port, job_id):
                from callables import *
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
            run_job_2, wait_job_2, save_job_result_2 = create_job_operators(cluster_name, port, '2')
            run_job_3, wait_job_3, save_job_result_3 = create_job_operators(cluster_name, port, '3')
            run_job_4, wait_job_4, save_job_result_4 = create_job_operators(cluster_name, port, '4')

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

            get_emr_virtual_cluster_id >> delete_emr_virtual_cluster >> delete_eks_cluster

            # create clusters
            create_eks_cluster >> create_iamidentitymapping >> update_trust_policy >> \
            create_emr_virtual_cluster >> add_kube_prometheus_stack >> \
            get_emr_virtual_cluster_id >> get_eks_arn >> get_kubeconfig >> port_forward >> \
            run_job_1 >> wait_job_1 >> save_job_result_1 >> \
            run_job_2 >> wait_job_2 >> save_job_result_2 >> \
            run_job_3 >> wait_job_3 >> save_job_result_3 >> \
            run_job_4 >> wait_job_4 >> save_job_result_4 >> \
            port_forward_stop >> delete_emr_virtual_cluster >> delete_eks_cluster
