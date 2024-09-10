import subprocess
import boto3
from datetime import datetime
import json


def get_emr_virtual_cluster_id_by_bash(**kwargs):
    cluster_name = kwargs['cluster_name']
    emr_virtual_cluster_name = f'{cluster_name}_emr_virtual_cluster'
    args = "aws emr-containers list-virtual-clusters --region ap-northeast-2 --query".split()
    args.append(f"virtualClusters[?name==`{emr_virtual_cluster_name}` && state==`RUNNING`].id")
    print(args)
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        case = result.stdout.strip()
        virtual_cluster_id = case[1:-1].strip()[1:-1]
        return {'virtual_cluster_id': virtual_cluster_id}


def delete_emr_virtual_cluster_func(**kwargs):
    # XCom에서 값을 가져옴
    '''

    :param kwargs:
        ti: task instance given by xcom
    :return:
    '''
    ti = kwargs['ti']
    virtual_cluster_id = ti.xcom_pull(task_ids='get_emr_virtual_cluster_id', key='return_value')['virtual_cluster_id']
    args = 'aws emr-containers delete-virtual-cluster --id'.split()
    args.extend([virtual_cluster_id])
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)


def run_job_func(**kwargs):
    '''
    :param kwargs:
        ti: task instance given by xcom
        job_run_id : string type number like '1', '2', '3'
    :return:
    '''
    # XCom에서 값을 가져옴
    cluster_name = kwargs['cluster_name']
    prefix = 'cluster_' + cluster_name + '.'
    virtual_cluster_id = kwargs['ti'].xcom_pull(task_ids=prefix+'get_emr_virtual_cluster_id', key='return_value')['virtual_cluster_id']
    job_run_id = kwargs['id']
    args = f'aws emr-containers start-job-run --cli-input-json file:///opt/airflow/config/job-run-{job_run_id}.json'.split()
    args.extend(["--virtual-cluster-id", virtual_cluster_id])
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
        data = json.loads(result.stdout)
        job_id = data.get('id')
        return {'job_id': job_id}
    if result.stderr:
        print(result.stderr)


def wait_job_done(**kwargs):
    import time
    ti = kwargs['ti']
    cluster_name = kwargs['cluster_name']
    prefix = 'cluster_' + cluster_name + '.'
    virtual_cluster_id = ti.xcom_pull(task_ids=prefix + "get_emr_virtual_cluster_id", key='return_value')['virtual_cluster_id']
    run_job_task_ids = 'run_job_' + kwargs['id']
    job_id = ti.xcom_pull(task_ids=prefix+run_job_task_ids, key='return_value')['job_id']
    args = ['aws', 'emr-containers', 'describe-job-run', '--id', job_id, '--virtual-cluster-id', virtual_cluster_id]
    while True:
        result = subprocess.run(args=args, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
            data = json.loads(result.stdout)
            state = data.get('jobRun').get('state')
            if state == "FAILED" or state == "CANCELLED":
                print(state)
                print('HERE 1', state, state == "FAILED", state == "CANCELLED")
                return False
            elif state == "COMPLETED":
                createdAt = data.get('jobRun').get('createdAt')
                finishedAt = data.get('jobRun').get('finishedAt')
                ti.xcom_push(key="createdAt", value=createdAt)
                ti.xcom_push(key="finishedAt", value=finishedAt)
                return True
            else:
                time.sleep(60)
                continue
        if result.stderr:
            print(result.stderr)
            print('HERE 2')
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

            # data.result에서 value의 두 번째 인자만 추출
            if "data" in data and "result" in data["data"]:
                for result in data["data"]["result"]:
                    # value 배열의 두 번째 인자 (CPU 사용률)를 추출
                    usage = result["value"][1]
        else:
            print(response.json())

        return usage

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
    prefix = 'cluster_' + cluster_name + '.'
    run_job_id = 'run_job_' + id
    wait_job_task_id = 'wait_job_' + id
    virtual_cluster_id = ti.xcom_pull(task_ids=prefix+'get_emr_virtual_cluster_id', key='return_value')['virtual_cluster_id']

    # Spend Time, Convert strings to datetime objects
    created_at = ti.xcom_pull(task_ids=prefix+wait_job_task_id, key='createdAt')
    finished_at = ti.xcom_pull(task_ids=prefix+wait_job_task_id, key='finishedAt')
    time_format = "%Y-%m-%dT%H:%M:%S%z"
    dt1 = datetime.strptime(created_at, time_format)
    dt2 = datetime.strptime(finished_at, time_format)
    spend_time = int((dt2 - dt1).total_seconds())

    # DataFrame Cluster, Job Name
    cluster_name = 'c1'
    job_name = run_job_id

    # CPU
    cpu_usage_query = f'100 * (1 - avg(rate(node_cpu_seconds_total{{mode="idle"}}[{spend_time}s])))'
    cpu_usage = get_usage(cpu_usage_query)
    print(cpu_usage, '%')

    # Network IO
    network_IO_usage_query = f'sum(rate(container_network_transmit_bytes_total[{spend_time}s])) + sum(rate(container_network_receive_bytes_total[{spend_time}s]))'
    network_IO_usage = get_usage(network_IO_usage_query)
    print(network_IO_usage, 'bytes')

    # Memory
    memory_usage_query = f'sum(avg_over_time(container_memory_usage_bytes[{spend_time}s]))'
    memory_usage = get_usage(memory_usage_query)
    print(memory_usage, 'bytes')

    # save results to s3
    s3 = boto3.client('s3')
    s3_bucket_name = 'middle-dataset'  # S3 버킷 이름
    s3_key = f'results/{virtual_cluster_id}/resource_usage.csv'  # S3에 저장될 파일 경로

    new_row = pd.DataFrame({
        'Cluster Name': [cluster_name],
        'Job Name': [job_name],
        'Spend Time': [spend_time],
        'CPU Usage': [cpu_usage],
        'Network IO Usage': [network_IO_usage],
        'Memory Usage': [memory_usage],
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


def set_port_forwarding(**kwargs):
    ti = kwargs['ti']
    cluster_name = kwargs['cluster_name']
    eks_arn = ti.xcom_pull(task_ids='cluster_'+cluster_name+'.get_eks_arn', key='return_value')
    port = kwargs['port']

    # kubeconfig 업데이트
    args = f"aws eks update-kubeconfig --name {cluster_name}"
    result = subprocess.run(args, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print('Error:', result.stderr)

    # 포트에서 실행 중인 프로세스 종료 (sudo 권한이 없다면 sudo를 제외)
    args = f"lsof -t -i:{port} | xargs kill -9"
    result = subprocess.run(args, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print('Error:', result.stderr)

    # 포트 포워딩 실행
    args = f"kubectl --context {eks_arn} port-forward prometheus-monitoring-{cluster_name}-k-prometheus-0 {port}:9090 &>/dev/null &"
    result = subprocess.run(args, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print('Error:', result.stderr)

    # Prometheus 쿼리 실행
    args = f"curl -G 'http://localhost:{port}/api/v1/query' --data-urlencode 'query=sum(rate(container_network_transmit_bytes_total[1h]))'"
    result = subprocess.run(args, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print('Error:', result.stderr)
