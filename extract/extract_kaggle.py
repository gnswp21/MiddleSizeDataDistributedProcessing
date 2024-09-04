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
# Kaggle Python API 인증 https://stackoverflow.com/questions/55934733/documentation-for-kaggle-api-within-python
# Kaggle Python API Usage https://www.kaggle.com/code/donkeys/kaggle-python-api#dataset_download_files()
kaggle_api = KaggleApi()
kaggle_api.authenticate()
logging.info('KaggleApi Authenticate SUCCESS')

s3 = boto3.client('s3', region_name='ap-northeast-2')
bucket_name = os.environ['BUCKET_NAME']

dataset = os.environ['DATASET']
files = kaggle_api.dataset_list_files(dataset).files
total = len(files)

# 파일 다운
logging.info(f"Total {total} files")
for i, file in enumerate(files):

    logging.info(f"NOW {i+1}/{total} file")
    file_name = file.name
    kaggle_api.dataset_download_file(dataset=dataset,
                                     file_name=file_name)
    # kaggle api는 파일의 크기가 크면 압축파일로 제공한다.
    file_name = file_name + '.zip'
    # 파일이 생성될 때까지 대기
    elapsed_time = 0
    max_wait_time = 1800
    while elapsed_time < max_wait_time:
        # 현재 디렉토리의 파일 목록 가져오기
        files = os.listdir()

        if file_name in files:
            logging.info(f"{file_name} is Found ")
            break

        # 1분(60초) 대기
        time.sleep(60)
        elapsed_time += 60
        logging.info(f" sleeping {elapsed_time} sec ...")
        if elapsed_time >= max_wait_time:
            logging.info(f" {file_name} is NOT Found until limit")
            break

    logging.info(f"Uploading {file_name} to S3...")
    s3.upload_file(file_name, bucket_name, file_name)
    # 파일 다운 후 삭제 메모리 용량보다 전체 파일의 합이 크기 때문
    logging.info(f"Delete local {file_name}")
    os.remove(file_name)
