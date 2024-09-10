from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 1)
}

with DAG(dag_id='create_cluster',
         description='create_cluster_dag',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:
    ## TODO insert cluster info by cluster spec
    # Create EKS Cluster
    create_eks_cluster = BashOperator(
        task_id='create_eks_cluster',  # task_id 수정 (공백 제거)
        bash_command='eksctl create cluster --name mid-cluster --with-oidc --instance-types=m5.xlarge --managed'
    )

    # Create IAM Identity Mapping
    create_iamidentitymapping = BashOperator(
        task_id='create_iamidentitymapping',
        bash_command='eksctl create iamidentitymapping --cluster mid-cluster --service-name "emr-containers" '
                     '--namespace default'
    )

    # Update Trust Policy
    update_trust_policy = BashOperator(
        task_id='update_trust_policy',
        bash_command='aws emr-containers update-role-trust-policy --cluster-name mid-cluster '
                     '--namespace default '
                     '--role-name EMRContainers-JobExecutionRole'
    )

    # Create EMR Virtual Cluster
    create_emr_virtual_cluster = BashOperator(
        task_id='create_emr_virtual_cluster',
        bash_command="""
        aws emr-containers create-virtual-cluster \
        --name mid_emr_virtual_cluster \
        --container-provider '{
            "id": "mid-cluster",
            "type": "EKS",
            "info": {
                "eksInfo": {
                    "namespace": "default"
                }
            }
        }'
        """
    )
    # TODO refactor cluster name by xcom
    # Add kube Prometheus stack by helm
    add_kube_prometheus_stack = BashOperator(
        task_id='add_kube_prometheus_stack',
        bash_command="""
        aws eks update-kubeconfig --name mid-cluster
        helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
        helm repo update
        helm install monitoring prometheus-community/kube-prometheus-stack
        """
    )



    create_eks_cluster >> create_iamidentitymapping >> update_trust_policy >> create_emr_virtual_cluster >> add_kube_prometheus_stack
