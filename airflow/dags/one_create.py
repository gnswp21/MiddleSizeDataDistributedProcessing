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

cluster_names = ['mid-cluster-1']
nodes =['2']
with DAG(dag_id='create_one_cluster',
         description='create_multiple_clusters_dag',
         default_args=default_args,
         schedule_interval=None,
         params={'tuning-id':1},
         catchup=False) as dag:

    for i in range(len(cluster_names)):
        cluster_name = cluster_names[i]
        node = nodes[i]
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

            create_eks_cluster >> create_iamidentitymapping >> update_trust_policy >> create_emr_virtual_cluster >> add_kube_prometheus_stack
