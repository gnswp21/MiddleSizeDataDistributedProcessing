import os
import subprocess
import boto3
from datetime import datetime
import json
from common import *


def get_emr_virtual_cluster_id_by_bash(**kwargs):
    cluster_name = kwargs['cluster_name']
    emr_virtual_cluster_name = f'{cluster_name}_emr_virtual_cluster'
    args = "aws emr-containers list-virtual-clusters --region ap-northeast-2 --query".split()
    args.append(f"virtualClusters[?name==`{emr_virtual_cluster_name}` && state==`RUNNING`].id")
    print(args)

    # run
    result = subprocess.run(args=args, capture_output=True, text=True)
    print_subprocess_result(result)
    if result.stdout:
        case = result.stdout.strip()
        virtual_cluster_id = case[1:-1].strip()[1:-1]
        return {'virtual_cluster_id': virtual_cluster_id}


def delete_emr_virtual_cluster_func(**kwargs):
    virtual_cluster_id = get_virtual_cluster_id(**kwargs)
    args = f'aws emr-containers delete-virtual-cluster --id {virtual_cluster_id}'
    result = subprocess.run(args=args, shell=True, capture_output=True, text=True)
    print_subprocess_result(result)


def run_job_func(**kwargs):
    # Get Variables From XCom
    cluster_name = kwargs['cluster_name']
    virtual_cluster_id = get_virtual_cluster_id(**kwargs)
    tuning_id = get_tuning_id(**kwargs)

    # tmp job_run_json
    tmp_job_run_json_path = modify_job_run_json(tuning_id, cluster_name, kwargs['id'])

    # AWS CLI 명령어 실행
    args = f"aws emr-containers start-job-run "\
           f"--cli-input-json file://{tmp_job_run_json_path} "\
           f"--virtual-cluster-id {virtual_cluster_id}"

    # Run aws emr-containers start-job-run
    result = subprocess.run(args=args, shell=True, capture_output=True, text=True)
    print_subprocess_result(result)
    # Check Result
    if result.stdout:
        data = json.loads(result.stdout)
        return {'aws_job_id': data['id']}


def wait_job_done(**kwargs):
    import time
    def check_job_run():
        args = ['aws', 'emr-containers', 'describe-job-run', '--id', aws_job_id, '--virtual-cluster-id', virtual_cluster_id]
        result = subprocess.run(args=args, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
            data = json.loads(result.stdout)
            state = data.get('jobRun').get('state')
            if state == "SUBMITTED":
                return "SUBMITTED"
            if state == "FAILED" or state == "CANCELLED":
                return "STOPPED"
            elif state == "COMPLETED":
                createdAt = data.get('jobRun').get('createdAt')
                finishedAt = data.get('jobRun').get('finishedAt')
                ti.xcom_push(key="createdAt", value=createdAt)
                ti.xcom_push(key="finishedAt", value=finishedAt)
                return "COMPLETED"
            else:
                return "RUNNING"
        if result.stderr:
            print(result.stderr)
            return "ERROR"

    def check_eks_pending():
        args = f'kubectl --kubeconfig /tmp/{cluster_name}_config ' \
               'get pods --field-selector=status.phase=Running -o custom-columns=NAME:.metadata.name | grep exec- | wc -l'
        running_executor_result = subprocess.run(args=args, shell=True, capture_output=True, text=True)
        if running_executor_result.stdout:
            running_executor = int(running_executor_result.stdout)
            print('running executor ', running_executor)
        if running_executor_result.stderr:
            print(running_executor_result.stdout)

        args = f'kubectl --kubeconfig /tmp/{cluster_name}_config ' \
               'get pods --field-selector=status.phase=Pending -o custom-columns=NAME:.metadata.name | grep exec- | wc -l'
        pending_executor_results = subprocess.run(args=args, shell=True, capture_output=True, text=True)

        if pending_executor_results.stdout:
            pending_executor = int(pending_executor_results.stdout)
            print('pending executor', pending_executor)

        if pending_executor_results.stderr:
            print(pending_executor_results.stdout)

        return pending_executor, running_executor

    ti = kwargs['ti']
    cluster_name = kwargs['cluster_name']
    virtual_cluster_id = get_virtual_cluster_id(**kwargs)
    prefix = get_prefix(**kwargs)
    job_id = kwargs['id']
    run_job_task_ids = f'run_job_{job_id}'
    aws_job_id = ti.xcom_pull(task_ids=prefix + run_job_task_ids, key='return_value')['aws_job_id']

    running_executor, pending_executor = 0, 0
    while True:
        state = check_job_run()
        if state == 'SUBMITTED':
            time.sleep(10)
        elif state =='RUNNING':
            p, r = check_eks_pending()
            pending_executor = max(pending_executor, p)
            running_executor = max(running_executor, r)
            ti.xcom_push(key="pending_executor", value=pending_executor)
            ti.xcom_push(key="running_executor", value=running_executor)
            if running_executor != 0 and pending_executor == 0:
                time.sleep(60)
            else:
                time.sleep(10)
        elif state =='COMPLETED':
            return True
        else: # state == 'STOPPED'
            return False



def save_job_result(**kwargs):
    from io import StringIO
    import requests
    import boto3
    import pandas as pd

    def get_usage(query):
        # HTTP GET 요청 보내기
        response = requests.get(prometheus_url, params={'query': query})
        usage = -1
        # 응답 결과 처리
        if response.status_code == 200:
            data = response.json()
            if "data" in data and "result" in data["data"]:
                for result in data["data"]["result"]:
                    # value 배열의 두 번째 인자 (CPU 사용률)를 추출
                    usage = result["value"][1]
        else:
            print(response.json())

        return str(round(float(usage), 4))
    # 파일이 존재하는지 확인하는 함수
    def check_file_exists(bucket, key):
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except:
            return False

    # S3에서 CSV 파일을 읽어오는 함수 (boto3 사용)
    def load_csv_from_s3(bucket, key):
        obj = s3.get_object(Bucket=bucket, Key=key)
        csv_data = obj['Body'].read().decode('utf-8')
        return csv_data

    # pandas를 사용하여 CSV 파일 수정
    def modify_csv_data(csv_data, new_row):
        # pandas로 기존 CSV 파일 읽기
        df = pd.read_csv(StringIO(csv_data))

        # 새로운 row 추가
        df = pd.concat([df, new_row], ignore_index=True)

        return df

    # S3에 CSV 파일을 저장하는 함수
    def save_csv_to_s3(df, bucket, key):
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())

    # Get Xcom Variables
    ti = kwargs['ti']
    id = kwargs['id']
    port = kwargs['port']

    # Prometheus API 엔드포인트 설정
    prometheus_url = f'http://localhost:{port}/api/v1/query'

    cluster_name = kwargs['cluster_name']
    prefix = get_prefix(**kwargs)
    job_id = 'job_' + id
    wait_job_task_id = 'wait_job_' + id

    # Spend Time, Convert strings to datetime objects
    created_at = ti.xcom_pull(task_ids=prefix + wait_job_task_id, key='createdAt')
    finished_at = ti.xcom_pull(task_ids=prefix + wait_job_task_id, key='finishedAt')
    time_format = "%Y-%m-%dT%H:%M:%S%z"
    dt1 = datetime.strptime(created_at, time_format)
    dt2 = datetime.strptime(finished_at, time_format)
    spend_time = int((dt2 - dt1).total_seconds())

    # DataFrame Cluster, Job Name
    job_name = job_id

    # Avg CPU rate
    avg_cpu_usage_query = f'100 * (1 - avg(rate(node_cpu_seconds_total{{mode="idle"}}[{spend_time}s])))'
    avg_cpu_usage = get_usage(avg_cpu_usage_query)
    print(avg_cpu_usage, '%')

    # Max CPU Rate
    max_cpu_usage_query = f'100 * (1 - min(rate(node_cpu_seconds_total{{mode="idle"}}[{spend_time}s])))'
    max_cpu_usage = get_usage(max_cpu_usage_query)
    print(max_cpu_usage, '%')

    # CPU USAGE
    node_num = pow(2, int(cluster_name[-1]))
    core_num = 4
    vcpu_usage = round(core_num * node_num * float(avg_cpu_usage) * int(spend_time), 4)
    print('vcpu_usage', vcpu_usage)

    # Memory
    memory_usage_query = f'sum(avg_over_time(container_memory_usage_bytes[{spend_time}s]))'
    memory_usage = get_usage(memory_usage_query)
    print('total memory usage', memory_usage, 'bytes')

    # Avg Memory Rate
    avg_memory_rate_usage_query = f'100 * (1 -avg(avg_over_time(node_memory_MemAvailable_bytes[{spend_time}]))/avg(node_memory_MemTotal_bytes))'
    avg_memory_rate_usage = get_usage(avg_memory_rate_usage_query)
    print('avg memory rate', avg_memory_rate_usage, '%')

    # Max Memory Rate
    max_memory_rate_usage_query = f'100 * (1 - min(min_over_time(node_memory_MemAvailable_bytes[{spend_time}s]))/avg(node_memory_MemTotal_bytes))'
    max_memory_rate_usage = get_usage(max_memory_rate_usage_query)
    print('max memory rate', max_memory_rate_usage, '%')

    # Network IO
    network_IO_usage_query = f'sum(rate(node_network_transmit_bytes_total[{spend_time}s])) + sum(rate(node_network_receive_bytes_total[{spend_time}s]))'
    network_IO_usage = get_usage(network_IO_usage_query)
    print('network IO', network_IO_usage, 'bytes')

    # executor
    pending_executor = ti.xcom_pull(task_ids=prefix + wait_job_task_id, key='pending_executor')
    running_executor = ti.xcom_pull(task_ids=prefix + wait_job_task_id, key='running_executor')
    print('pending executor', pending_executor)
    print('running executor', running_executor)

    # save results to s3
    s3 = boto3.client('s3')
    s3_bucket_name = 'middle-dataset'  # S3 버킷 이름
    tuning_id = get_tuning_id(**kwargs)

    s3_key = f'results/tuning-{tuning_id}/{cluster_name}_resource_usage.csv'  # S3에 저장될 파일 경로

    new_row = pd.DataFrame({
        'Cluster Name': [cluster_name],
        'Job Name': [job_name],
        'Spend Time (s)': [spend_time],
        'AVG CPU (%)': [avg_cpu_usage],
        'MAX CPU (%)': [max_cpu_usage],
        'Total vCPU Usage (s)': [vcpu_usage],
        'AVG Memory Rate (%)': [avg_memory_rate_usage],
        'MAX Memory Rate (%)': [max_memory_rate_usage],
        'Total Memory Usage (Byte)': [memory_usage],
        'Network IO (Byte)': [network_IO_usage],
        'Pending Executor': [pending_executor],
        'Running Executor': [running_executor]
    })

    # S3에 파일이 존재하는지 확인
    if check_file_exists(s3_bucket_name, s3_key):
        # 파일이 존재하면 기존 내용을 가져와 row를 추가
        print("파일이 존재합니다. 기존 파일에 row를 추가합니다.")

        # S3에서 CSV 파일 읽어오기
        csv_data = load_csv_from_s3(s3_bucket_name, s3_key)

        # pandas로 CSV 수정 (새로운 row 추가)
        df = modify_csv_data(csv_data, new_row)

        # 수정된 파일을 다시 S3에 업로드
        save_csv_to_s3(df, s3_bucket_name, s3_key)
    else:
        # 파일이 존재하지 않으면 새로 생성 후 row 추가
        print("파일이 존재하지 않습니다. 새 파일을 생성합니다.")

        # 새로운 DataFrame 생성
        # df = pd.DataFrame(new_row)
        df = new_row

        # S3에 업로드
        save_csv_to_s3(df, s3_bucket_name, s3_key)

    print(f"Data successfully uploaded to s3://{s3_bucket_name}/{s3_key}")


