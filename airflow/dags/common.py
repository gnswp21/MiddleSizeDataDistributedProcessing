import os
import subprocess
import boto3
from datetime import datetime
import json


def get_virtual_cluster_id(**kwargs):
    ti = kwargs['ti']
    cluster_name = kwargs['cluster_name']
    prefix = 'cluster_' + cluster_name + '.'
    virtual_cluster_id = ti.xcom_pull(task_ids=prefix + 'get_emr_virtual_cluster_id', key='return_value')[
        'virtual_cluster_id']

    return virtual_cluster_id

def get_prefix(**kwargs):
    ti = kwargs['ti']
    prefix = get_prefix(**kwargs)
    return ti.xcom_pull(task_ids=prefix+'run_job_'+kwargs['id'], key='return_value')['job_id']



def get_tuning_id(**kwargs):
    tuning_id = kwargs['param']['tuning-id']
    return tuning_id


def modify_job_run_json(tuning_id, cluster_name, id):
    # JSON 튜닝 컨피그 파일 로드
    tuning_file_path = f'/opt/airflow/config/{tuning_id}.json'
    with open(tuning_file_path, 'r') as file:
        tuning_config = json.load(file)

    entryPointArguments = tuning_config[{cluster_name}][f'job-{id}']['entryPointArguments']
    sparkSubmitParameters = ('--py-files local:///etl/dependency_packages.zip ' +
                             tuning_config[{cluster_name}][f'job-{id}']['sparkSubmitParameters'])

    # JSON job run 파일 로드
    run_job_file_path = f'/opt/airflow/config/job-run.json'
    with open(run_job_file_path, 'r') as file:
        job_run_config = json.load(file)

    # entryPointArguments, sparkSubmitParameters 추가
    job_run_config['jobDriver']['sparkSubmitJobDriver']['entryPointArguments'] = entryPointArguments
    job_run_config['jobDriver']['sparkSubmitJobDriver']['sparkSubmitParameters'] = sparkSubmitParameters

    # S3 access key, secret key 추가
    for app_config in job_run_config['configurationOverrides']['applicationConfiguration']:
        if app_config['classification'] == 'spark-defaults':
            app_config['properties']['spark.hadoop.fs.s3a.access.key'] = os.getenv('AWS_ACCESS_KEY_ID')
            app_config['properties']['spark.hadoop.fs.s3a.secret.key'] = os.getenv('AWS_SECRET_ACCESS_KEY')

    # 수정된 JSON 파일 저장
    tmp_run_job_file_path = f'/tmp/{cluster_name}-job-{id}.json'
    with open(tmp_run_job_file_path, 'w') as file:
        json.dump(job_run_config, file)
    return tmp_run_job_file_path

def print_subprocess_result(result):
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)