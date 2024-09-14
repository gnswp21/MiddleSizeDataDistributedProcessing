import os
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
    cluster_name = kwargs['cluster_name']
    prefix = 'cluster_' + cluster_name + '.'
    virtual_cluster_id = ti.xcom_pull(task_ids=prefix + 'get_emr_virtual_cluster_id', key='return_value')[
        'virtual_cluster_id']
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
    # Get Variables From XCom
    ti = kwargs['ti']
    cluster_name = kwargs['cluster_name']
    prefix = 'cluster_' + cluster_name + '.'
    job_run_id = kwargs['id']
    virtual_cluster_id = ti.xcom_pull(task_ids=prefix + 'get_emr_virtual_cluster_id',
                                      key='return_value')['virtual_cluster_id']

    # JSON 파일 경로
    run_job_file_path = f'/opt/airflow/config/{cluster_name}/job-run-{job_run_id}.json'
    # JSON 파일 로드
    with open(run_job_file_path, 'r') as file:
        job_run_config = json.load(file)

    # S3 access key, secret key 추가
    for app_config in job_run_config['configurationOverrides']['applicationConfiguration']:
        if app_config['classification'] == 'spark-defaults':
            app_config['properties']['spark.hadoop.fs.s3a.access.key'] = os.getenv('AWS_ACCESS_KEY_ID')
            app_config['properties']['spark.hadoop.fs.s3a.secret.key'] = os.getenv('AWS_SECRET_ACCESS_KEY')

    # 수정된 JSON 파일 저장
    tmp_run_job_file_path = f'/tmp/job-run-{job_run_id}.json'
    with open(tmp_run_job_file_path, 'w') as file:
        json.dump(job_run_config, file)

    # AWS CLI 명령어 실행
    args = f"aws emr-containers start-job-run".split()
    args.extend(["--cli-input-json", f'file://' + tmp_run_job_file_path])
    args.extend(["--virtual-cluster-id", virtual_cluster_id])

    # Run aws emr-containers start-job-run
    result = subprocess.run(args=args, capture_output=True, text=True)

    # Check Result
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
    job_id = ti.xcom_pull(task_ids=prefix + 'run_job_' + kwargs['id'], key='return_value')['job_id']
    args = ['aws', 'emr-containers', 'describe-job-run', '--id', job_id, '--virtual-cluster-id', virtual_cluster_id]
    # kubeconfig 파일 경로 설정
    kubeconfig_path = f'/tmp/{cluster_name}_config'

    # STATUS가 Pending인 파드 목록을 가져오기 위한 함수
    def get_pending_pods(kubeconfig):
        try:
            result = subprocess.run(
                ["kubectl", "get", "pods", "--kubeconfig", kubeconfig, "--field-selector=status.phase=Pending", "--no-headers", "-o", "custom-columns=:metadata.name"],
                check=True, capture_output=True, text=True
            )
            return result.stdout.splitlines()  # 파드 이름을 리스트로 반환
        except subprocess.CalledProcessError as e:
            print(f"Error while getting pending pods: {e}")
            return []

    # Pending 상태인 파드에 대해 describe 명령어 실행 함수
    def describe_pod(pod_name, kubeconfig):
        try:
            subprocess.run(
                ["kubectl", "describe", "pod", pod_name, "--kubeconfig", kubeconfig],
                check=True
            )
        except subprocess.CalledProcessError as e:
            print(f"Error while describing pod {pod_name}: {e}")



    while True:
        pending_pods = get_pending_pods(kubeconfig_path)
        if pending_pods:
            for pod in pending_pods:
                print(f"Describing pod: {pod}")
                describe_pod(pod, kubeconfig_path)
        else:
            print("No pending pods found.")

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
    prefix = 'cluster_' + cluster_name + '.'
    run_job_id = 'run_job_' + id
    wait_job_task_id = 'wait_job_' + id
    virtual_cluster_id = ti.xcom_pull(task_ids=prefix + 'get_emr_virtual_cluster_id', key='return_value')[
        'virtual_cluster_id']

    # Spend Time, Convert strings to datetime objects
    created_at = ti.xcom_pull(task_ids=prefix + wait_job_task_id, key='createdAt')
    finished_at = ti.xcom_pull(task_ids=prefix + wait_job_task_id, key='finishedAt')
    time_format = "%Y-%m-%dT%H:%M:%S%z"
    dt1 = datetime.strptime(created_at, time_format)
    dt2 = datetime.strptime(finished_at, time_format)
    spend_time = int((dt2 - dt1).total_seconds())

    # DataFrame Cluster, Job Name
    job_name = run_job_id

    # Avg CPU rate
    avg_cpu_usage_query = f'100 * (1 - avg(rate(node_cpu_seconds_total{{mode="idle"}}[{spend_time}s])))'
    avg_cpu_usage = get_usage(avg_cpu_usage_query)
    print('avg_cpu_usage' ,avg_cpu_usage, '%')

    # Max CPU Rate
    max_cpu_usage_query = f'100 * (1 - min(rate(node_cpu_seconds_total{{mode="idle"}}[{spend_time}s])))'
    max_cpu_usage = get_usage(max_cpu_usage_query)
    print('max_cpu_usage', max_cpu_usage, '%')

    # CPU USAGE
    node_num = pow(2, int(cluster_name[-1]))
    core_num = 4
    vcpu_usage = round(core_num * node_num * float(avg_cpu_usage) * int(spend_time), 4)
    print('vcpu_usage', vcpu_usage)



    # Avg Memory Rate
    avg_memory_rate_usage_query = f'100 * (1 -avg(avg_over_time(node_memory_MemAvailable_bytes[{spend_time}]))/avg(node_memory_MemTotal_bytes))'
    avg_memory_rate_usage = get_usage(avg_memory_rate_usage_query)
    print('avg_memory_rate_usage', avg_memory_rate_usage, '%')

    # Max Memory Rate
    max_memory_rate_usage_query = f'100 * (1 - min(min_over_time(node_memory_MemAvailable_bytes[{spend_time}s]))/avg(node_memory_MemTotal_bytes))'
    max_memory_rate_usage = get_usage(max_memory_rate_usage_query)
    print('max_memory_rate_usage', max_memory_rate_usage, '%')

    # Memory
    memory_usage_query = f'sum(avg_over_time(container_memory_usage_bytes[{spend_time}s]))'
    memory_usage = get_usage(memory_usage_query)
    print('memory_usage', memory_usage, 'bytes')

    # Avg Memory Rate by pod
    pod_avg_memory_rate_usage_query = f'100 * (1 -avg(avg_over_time(node_memory_MemAvailable_bytes[{spend_time}]))/avg(node_memory_MemTotal_bytes))'
    pod_avg_memory_rate_usage = get_usage(pod_avg_memory_rate_usage_query)
    print('pod_avg_memory_rate_usage', pod_avg_memory_rate_usage, '%')

    # Max Memory Rate by pod
    #'max_over_time((container_memory_usage_bytes{container!="",pod!=""} / container_spec_memory_limit_bytes{container!="",pod!=""}) * 100 [1h])'
    pod_max_memory_rate_usage_query = f'max_over_time((container_memory_usage_bytes{{container!="",pod!=""}} / container_spec_memory_limit_bytes{{container!="",pod!=""}}) * 100 [{spend_time}s])'
    pod_max_memory_rate_usage = get_usage(pod_max_memory_rate_usage_query)
    print('pod_max_memory_rate_usage', pod_max_memory_rate_usage, '%')

    # Network IO
    network_IO_usage_query = f'sum(rate(node_network_transmit_bytes_total[{spend_time}s])) + sum(rate(node_network_receive_bytes_total[{spend_time}s]))'
    network_IO_usage = get_usage(network_IO_usage_query)
    print('network_IO_usage', network_IO_usage, 'bytes')

    # save results to s3
    s3 = boto3.client('s3')
    s3_bucket_name = 'middle-dataset'  # S3 버킷 이름
    s3_key = f'results/{cluster_name}/{virtual_cluster_id}/resource_usage.csv'  # S3에 저장될 파일 경로

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


def set_eks_config(**kwargs):
    ti = kwargs['ti']
    cluster_name = kwargs['cluster_name']
    eks_arn = ti.xcom_pull(task_ids='cluster_' + cluster_name + '.get_eks_arn', key='return_value')
    port = kwargs['port']

    # kubeconfig 업데이트
    args = f"aws eks update-kubeconfig --name {cluster_name}"
    print(args)
    result = subprocess.run(args, shell=True, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print('Error:', result.stderr)

    # 포트에서 실행 중인 프로세스 종료 (sudo 권한이 없다면 sudo를 제외)
    # args = f"lsof -t -i:{port} | xargs kill -9"
    # print(args)
    # result = subprocess.run(args, shell=True, capture_output=True, text=True)
    # if result.stdout:
    #     print(result.stdout)
    # if result.stderr:
    #     print('Error:', result.stderr)

    # 포트 포워딩 실행
    # args = f"kubectl --context {eks_arn} port-forward prometheus-monitoring-{cluster_name}-k-prometheus-0 {port}:9090 &>/dev/null &"
    # print(args)
    # result = subprocess.run(args, shell=True, capture_output=True, text=True)
    # if result.stdout:
    #     print(result.stdout)
    # if result.stderr:
    #     print('Error:', result.stderr)

    # Prometheus 쿼리 실행
    # args = f"curl -G 'http://localhost:{port}/api/v1/query' --data-urlencode 'query=sum(rate(container_network_transmit_bytes_total[1h]))'"
    # print(args)
    # result = subprocess.run(args, shell=True, capture_output=True, text=True)
    # if result.stdout:
    #     print(result.stdout)
    # if result.stderr:
    #     print('Error:', result.stderr)
