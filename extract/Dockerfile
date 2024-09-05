# Python 3.8 이미지 사용
FROM python:3.8-slim

# 작업 디렉토리 설정 (옵션, 필요에 따라 변경 가능)
WORKDIR /etl

# 필요한 패키지 설치
RUN pip install --no-cache-dir kaggle boto3

#
COPY extract/src /etl/extract/src

# 기본 명령어 설정 (옵션, 필요에 따라 변경 가능)
