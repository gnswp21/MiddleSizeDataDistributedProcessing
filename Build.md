# MiddleSizeDataDistributedProcessing
중간 규모(50기가)의 데이터를 emr on eks로 분산 처리 효율성을 분석한다.

# send2s3
```commandline
docker build -t extract -f ./extract/Dockerfile .
docker compose -f ./extract/docker-compose.yml up -d
docker logs -f extract
docker compose -f ./process_dev/docker-compose.yml down
```

# airflow
```commandline
docker compose -f ./airflow/docker-compose.yml up -d --build
```
# Build Dockerfile & Send to dkr.ecr
```bash
# For docker build login seoul dcr
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 996579266876.dkr.ecr.ap-northeast-2.amazonaws.com
# For docker push login my dcr
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 691487686124.dkr.ecr.ap-northeast-2.amazonaws.com
docker build -t emr6.5_mid -f .\process\Dockerfile .
docker tag emr6.5_mid 691487686124.dkr.ecr.ap-northeast-2.amazonaws.com/emr6.5_mid_repo
docker push 691487686124.dkr.ecr.ap-northeast-2.amazonaws.com/emr6.5_mid_repo
```

# Airflow Dag 실행
에어플로우 웹 유아이에 접속,
create_run_delete_all DAG 실행, 원하는 튜닝버전을 지정하기 위한 파라미터 입력
결과 s3 통해 확인




# 푸시파일 to airflow
```commandline
pscp -P 3323 -i C:\Users\family\Projects\ec2-putty-key.ppk airflow/config/tuning-1.json ubuntu@13.209.6.57:/home/ubuntu/MiddleSizeDataDistributedProcessing/airflow/config
pscp -P 3323 -i C:\Users\family\Projects\ec2-putty-key.ppk airflow/dags/create_run_delete_all.py ubuntu@13.209.6.57:/home/ubuntu/MiddleSizeDataDistributedProcessing/airflow/dags
pscp -P 3323 -i C:\Users\family\Projects\ec2-putty-key.ppk airflow/dags/callables.py ubuntu@13.209.6.57:/home/ubuntu/MiddleSizeDataDistributedProcessing/airflow/dags
pscp -P 3323 -i C:\Users\family\Projects\ec2-putty-key.ppk airflow/dags/common.py ubuntu@13.209.6.57:/home/ubuntu/MiddleSizeDataDistributedProcessing/airflow/dags
```

