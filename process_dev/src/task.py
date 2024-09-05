from pyspark.sql.functions import col, substring
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql import SparkSession
from typing import Any, Dict
import sys
import logging
import zipfile
from io import BytesIO
from pyspark.sql import DataFrame


def task1(df: DataFrame):
    new_df = (df.withColumn('First', substring('value', 1, 1))
              .groupBy('First')
              .count()
              .sort('First'))

    new_df.show()


def task1(df: DataFrame):
    new_df = (df.withColumn('First', substring('value', 1, 1))
              .groupBy('First')
              .count()
              .sort('First'))

    new_df.show()


def run(kwargs: Dict[Any, Any]):
    spark = SparkSession.builder.appName(f"{kwargs['job_name']}").getOrCreate()

    # Set Logging    
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')
    logging.info('Spark Session Build SUCCESS')

    # Add custom dependency
    spark.sparkContext.addPyFile("/etl/dependency_packages.zip")

    # make dev dataset
    data = [["123123"],["asdasd"], ["agfasdfasd"]]
    schema = StructType([StructField("value", StringType(), True)])
    df = spark.createDataFrame(data=data, schema=schema)
    task1(df)

    spark.stop()


if __name__ == "__main__":
    kwargs = dict(zip(["job_name"], sys.argv[1:]))
    run(kwargs)
