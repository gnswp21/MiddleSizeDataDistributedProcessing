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


def run(kwargs: Dict[Any, Any]):
    spark = SparkSession.builder.appName(f"{kwargs['job_name']}").getOrCreate()
    dataset = kwargs['dataset']
    task_id = kwargs['task_id']

    # Set Logging    
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Spark Session Build SUCCESS')

    # Add custom dependency
    spark.sparkContext.addPyFile("/etl/dependency_packages.zip")

    df = None
    if dataset == 'dev-dataset':
        # make dev dataset
        data = [["123123"],["asdasd"], ["agfasdfasd"]]
        schema = StructType([StructField("value", StringType(), True)])
        df = spark.createDataFrame(data=data, schema=schema)
    elif dataset == '50':
        pass
    elif dataset == '150':
        pass

    if not df:
        logging.warnning('!!! USE Correct Dataset')

    if task_id == 1:
        task1(df)
    elif task_id == 2:
        # NLTK 사전 로드 (드라이버에서 한 번만 실행)
        nltk.download('words')
        english_vocab = set(words.words())
        # 브로드캐스트 변수로 사전 전파
        broadcast_vocab = spark.sparkContext.broadcast(english_vocab)
        task2(df, broadcast_vocab)

    spark.stop()


if __name__ == "__main__":
    kwargs = dict(zip(["job_name", "dataset", "task_id"], sys.argv[1:]))
    run(kwargs)
