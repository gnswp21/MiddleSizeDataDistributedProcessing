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
kaggle_api.dataset_download_files(dataset=dataset, unzip=True)
with open('rockyou2024.zip', 'wb') as output_file:
    for i in range(1, 12):  # 파일 번호에 맞춰 범위 설정
        with open(f'rockyou2024.zip.{i:03}', 'rb') as input_file:
            output_file.write(input_file.read())

if os.path.exists('rockyou2024.zip'):
    with zipfile.ZipFile('rockyou2024.zip', 'r') as zip_ref:
        zip_ref.extractall('rockyou2024')
    
# file check
if not os.path.exists('rockyou2024'):
    logging.info('no rockyou2024')
    logging.info(f'{os.listdir()}')
    sys.exit(-1)
    
# set varables    
s3 = boto3.client('s3', region_name='ap-northeast-2')
file_path = 'rockyou2024'
key = 'rockyou2024'

# 멀티파트 업로드 시작
# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_multipart_upload.html
# https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
multipart_upload = s3.create_multipart_upload(Bucket=bucket_name, Key=key)

# 파일 크기 확인
file_size = os.path.getsize(file_path)
part_size = 100 * 1024 * 1024  # 각 파트 크기를 100MB로 설정
parts = []

with open(file_path, 'rb') as f:
    part_number = 1
    while chunk := f.read(part_size):
        response = s3.upload_part(
            Bucket=bucket_name,
            Key=key,
            PartNumber=part_number,
            UploadId=multipart_upload['UploadId'],
            Body=chunk
        )
        parts.append({'PartNumber': part_number, 'ETag': response['ETag']})
        part_number += 1

# 모든 파트를 완료
s3.complete_multipart_upload(
    Bucket=bucket_name,
    Key=key,
    UploadId=multipart_upload['UploadId'],
    MultipartUpload={'Parts': parts}
)