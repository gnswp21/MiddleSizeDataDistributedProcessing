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


def task1(df: DataFrame):
    new_df = (df.withColumn('First', substring('value', 1, 1))
              .groupBy('First')
              .count()
              .sort('First'))

    new_df.show()


def task2(df: DataFrame, broadcast_vocab):
    '''
    8 ~ 32 글자
    1개 이상의 대문자
    1개 이상의 소문자
    1개 이상의 숫자
    1개 이상의 특수문자
    사전에 등재된 단어 아님
    '''
    @udf()
    def is_valid_password(value):
        # 양 끝 공백 제거
        value = value.strip()

        # 조건 1: 길이가 8~32자 사이여야 함
        if len(value) < 8 or len(value) > 32:
            return False

        # 미리 정의된 문자 집합들
        uppercase = set("ABCDEFGHIJKLMNOPQRSTUVWXYZ")
        lowercase = set("abcdefghijklmnopqrstuvwxyz")
        digits = set("0123456789")
        # 기본 특수 문자 (string.punctuation)
        special_characters = set(string.punctuation)

        # 패스워드를 문자 집합으로 변환
        value_set = set(value)

        # 조건 2: 하나 이상의 대문자 포함
        if not value_set & uppercase:
            return False

        # 조건 3: 하나 이상의 소문자 포함
        if not value_set & lowercase:
            return False

        # 조건 4: 하나 이상의 숫자 포함
        if not value_set & digits:
            return False

        # 조건 5: 하나 이상의 특수 문자 포함
        if not value_set & special_characters:
            return False

        # 조건 6: 단어가 사전에 존재하지 않는지 검사
        if value.lower() in broadcast_vocab.value:
            return False

        # 모든 조건을 통과하면 True
        return True

    new_df = df.withColumn('StrongPassword', is_valid_password(col('value')))
    new_df.show()