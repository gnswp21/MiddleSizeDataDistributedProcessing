from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SparkSession
from typing import Any, Dict
import sys
import logging




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
    s3_input_path = "s3a://middle-dataset/rockyou2024.zip.001.zip"
    # schema = StructType([
    #     StructField("page", StringType(), True),
    #     StructField("level", StringType(), True),
    #     StructField("name", StringType(), True)
    # ])
    # df = spark.read.csv(s3_input_path, schema=schema, header=False)

    
    spark.stop()


if __name__ == "__main__":
    kwargs = dict(zip(["job_name"], sys.argv[1:]))
    run(kwargs)