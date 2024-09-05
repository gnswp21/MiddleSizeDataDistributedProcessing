import boto3
import logging
import os
import time
import sys
import zipfile

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

bucket_name = os.environ['BUCKET_NAME']

dataset = os.environ['DATASET']
files = kaggle_api.dataset_list_files(dataset).files
total = len(files)

logging.info(f"Total {total} files")
# 파일 다운
kaggle_api.dataset_download_files(dataset=dataset, unzip=True, path='data')