import boto3
import logging
import os
import time

from kaggle.api.kaggle_api_extended import KaggleApi

###
# BEFORE RUN
# 1.  SET env variables KAGGLE_USERNAME, KAGGLE_KEY
# 2.  SET env variables AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
###

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('')
s3 = boto3.client('s3', region_name='ap-northeast-2')
bucket_name = os.environ['BUCKET_NAME']
dataset = os.environ['DATASET']

path = '/etl/data'
dirs = os.listdir(path)
for dir_name in dirs:
    files = os.listdir(path+'/'+dir_name)
    for file_name in files:
        print(file_name)




# 파일 다운
# logging.info(f"Uploading {file_name} to S3...")
# s3.upload_file(file_name, bucket_name, file_name)
# # 파일 다운 후 삭제 메모리 용량보다 전체 파일의 합이 크기 때문
# logging.info(f"Delete local {file_name}")
