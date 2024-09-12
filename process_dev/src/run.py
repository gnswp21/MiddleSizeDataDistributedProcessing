import string
from pyspark.sql.functions import col, substring, udf
from pyspark.sql.types import StringType, StructType, StructField, BooleanType
from pyspark.sql import SparkSession
from typing import Any, Dict
import sys
import logging
import zipfile
from io import BytesIO
from pyspark.sql import DataFrame
import nltk
from nltk.corpus import words
from task import *
from run import *


def run(kwargs: Dict[Any, Any]):
    spark = SparkSession.builder.appName(f"{kwargs['job_name']}").getOrCreate()
    # Set argv
    dataset = kwargs['dataset']
    task_id = kwargs['task_id']
    cluster_name = kwargs['cluster_name']
    job_id = kwargs['job_id']
    bucket = 's3a://middle-dataset/'

    # Set Logging
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Spark Session Build SUCCESS')

    # Add custom dependency
    spark.sparkContext.addPyFile("/etl/dependency_packages.zip")

    # Get Dataset From S3
    df = None
    if dataset == 'one':
        path = bucket + 'data/Z.txt'
        df = spark.read.text(paths=path)
    elif dataset == 'all':
        path = bucket + 'data'
        df = spark.read.text(paths=path)
    else:
        logging.warnning('!!! USE Correct Dataset')
        data = [["1234567890"],["abdedfg"], ["!@#$"]]
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(data=data, schema=schema)

    if task_id == '1':
        df = task1(df)
    elif task_id == '2':
        # NLTK 사전 로드 (드라이버에서 한 번만 실행)
        nltk.download('words')
        english_vocab = set(words.words())
        # 브로드캐스트 변수로 사전 전파
        broadcast_vocab = spark.sparkContext.broadcast(english_vocab)
        df= task2(spark, df, broadcast_vocab)

    result_path = bucket + f'results/{cluster_name}/{job_id}_result.csv'
    df.show()
    df.write.csv(path=result_path,mode='overwrite')
    spark.stop()


if __name__ == "__main__":
    kwargs = dict(zip(["job_name", "dataset", "task_id", "cluster_name", "job_id"], sys.argv[1:]))
    run(kwargs)
