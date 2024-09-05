import boto3
import logging
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logging.info('')
s3 = boto3.client('s3', region_name='ap-northeast-2')
bucket_name = os.environ['BUCKET_NAME']
dataset = os.environ['DATASET']

path = '/etl/data'
dirs = os.listdir(path)
dirs.sort()
for dir_name in dirs:
    files = os.listdir(path+'/'+dir_name)
    for file_name in files:
        logging.info(f"Uploading {file_name} to S3...")
        try:
            response = s3.upload_file(Filename=path+'/'+dir_name+'/'+file_name, Bucket=bucket_name, Key=file_name)
        except Exception as e:
            logging.error(e)
