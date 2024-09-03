import boto3
import logging
import os
from kaggle.api.kaggle_api_extended import KaggleApi

###
# BEFORE RUN
# 1.  SET env variables KAGGLE_USERNAME, KAGGLE_KEY
# 2.  SET env variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
###

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# Kaggle Python API 인증 https://stackoverflow.com/questions/55934733/documentation-for-kaggle-api-within-python
# Kaggle Python API Usage https://www.kaggle.com/code/donkeys/kaggle-python-api#dataset_download_files()
kaggle_api = KaggleApi()
kaggle_api.authenticate()
logging.info('KaggleApi Authenticate SUCCESS')

s3 = boto3.client('s3', region_name='ap-northeast-2')
bucket_name = os.environ['BUCKET_NAME']
# bucket_name = 'middle-dataset'

# dataset = 'haseebindata/student-performance-predictions'
dataset = os.environ['DATASET']
files = kaggle_api.dataset_list_files(dataset).files

# 파일 다운
for file in files:
    file_name = file.name
    kaggle_api.dataset_download_file(dataset, file_name)
    logging.info(f"Uploading {file_name} to S3...")
    s3.upload_file(file_name, bucket_name, file_name)
    # 파일 다운 후 삭제 메모리 용량보다 전체 파일의 합이 크기 때문
    os.remove(file_name)
    logging.info(f"Delete local {file_name}")
