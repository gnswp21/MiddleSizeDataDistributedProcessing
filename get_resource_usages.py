from io import StringIO

import requests
import json
import boto3
import pandas as pd

# Prometheus API 엔드포인트 설정
prometheus_url = 'http://localhost:9090/api/v1/query'

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


# Cluster, Job Name
cluster_name = 'c1'
job_name = 'j1'

# CPU
spend_time = 3600
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
s3_key = 'results/resource_usage.csv'  # S3에 저장될 파일 경로


new_row = pd.DataFrame({
    'Cluster Name': [cluster_name],
    'Job Name': [job_name],
    'CPU Usage': [cpu_usage],
    'Network IO Usage': [network_IO_usage],
    'Memory Usage': [memory_usage]
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
    df = pd.DataFrame([new_row])

    # S3에 업로드
    save_csv_to_s3(df, s3_bucket_name, s3_key)

print(f"Data successfully uploaded to s3://{s3_bucket_name}/{s3_key}")