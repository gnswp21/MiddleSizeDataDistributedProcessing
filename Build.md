# MiddleSizeDataDistributedProcessing
중간 규모(50기가)의 데이터를 emr on eks로 분산 처리 효율성을 분석한다.

# .env 파일 관리
## airflow (~/airflow/.env)
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- AWS_DEFAULT_REGION
- AWS_DEFAULT_OUTPUT
- AIRFLOW_UID

## extract (~/extarct/.env)
- KAGGLE_USERNAME
- KAGGLE_KEY
- DATASET
- AWS_ACCESS_KEY_ID
- AWS_SECRET_ACCESS_KEY
- BUCKET_NAME

## process_dev (~/process_dev/.env)
- SPARK_HADOOP_FS_S3A_ACCESS_KEY
- SPARK_HADOOP_FS_S3A_SECRET_KEY

# extract
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

# Process
```bash
# For docker build login seoul dcr
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 996579266876.dkr.ecr.ap-northeast-2.amazonaws.com
# For docker push login my dcr
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 691487686124.dkr.ecr.ap-northeast-2.amazonaws.com
docker build -t emr6.5_mid -f .\process\Dockerfile .
docker tag emr6.5_mid 691487686124.dkr.ecr.ap-northeast-2.amazonaws.com/emr6.5_mid_repo
docker push 691487686124.dkr.ecr.ap-northeast-2.amazonaws.com/emr6.5_mid_repo
```


# 푸시파일 to airflow
```commandline
pscp -P 3323 -i ${PUTTY_KEY} airflow/config/tuning-1.json ubuntu@13.209.6.57:/home/ubuntu/MiddleSizeDataDistributedProcessing/airflow/config
pscp -P 3323 -i ${PUTTY_KEY} airflow/dags/create_run_delete_all.py ubuntu@13.209.6.57:/home/ubuntu/MiddleSizeDataDistributedProcessing/airflow/dags
```

