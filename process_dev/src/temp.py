from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SparkSession
from typing import Any, Dict
import sys
import logging
import zipfile
from io import BytesIO



def run(kwargs: Dict[Any, Any]):
    spark = (
        SparkSession.builder.appName(f"{kwargs['job_name']}")
        .getOrCreate()
    )
    
    logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
    
    logging.info('Spark Session Build SUCCESS')

    spark.sparkContext.addPyFile("/etl/dependency_packages.zip")

    # Read df from S3
    # S3에서 ZIP 파일 읽기
    s3_path = "s3a://middle-dataset/rockyou2024.zip.011.zip"

    # 파일을 읽어서 메모리에 올리기
    zip_file = spark.read.format("binaryFile").load(s3_path)

    # ZIP 파일 압축 해제 (zipfile 사용)
    zip_binary_data = zip_file.collect()[0][0]
    with zipfile.ZipFile(BytesIO(zip_binary_data), 'r') as zip_ref:
        # 압축 파일 내의 파일 목록 확인
        zip_ref.printdir()
        
        # 압축 해제된 파일 읽기 (첫 번째 파일 예시)
        extracted_file = zip_ref.open(zip_ref.namelist()[0])
        
        # PySpark로 CSV 등 파일 형식에 맞게 처리
        extracted_df = spark.read.csv(BytesIO(extracted_file.read()), header=True, inferSchema=True)

    # DataFrame 작업 수행
    extracted_df.show()

    
    spark.stop()


if __name__ == "__main__":
    kwargs = dict(zip(["job_name"], sys.argv[1:]))
    run(kwargs)