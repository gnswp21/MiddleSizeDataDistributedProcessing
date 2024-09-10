import subprocess
import boto3
import json



def get_emr_virtual_cluster_id_by_bash():
    args = "aws emr-containers list-virtual-clusters --region ap-northeast-2 --query".split()
    args.append('virtualClusters[?name==`mid_emr_virtual_cluster` && state==`RUNNING`].id')
    print(args)
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        case = result.stdout.strip()
        virtual_cluster_id = case[1:-1].strip()[1:-1]
        return {'virtual_cluster_id': virtual_cluster_id}


def delete_emr_virtual_cluster_func(**kwargs):
    # XCom에서 값을 가져옴
    '''

    :param kwargs:
        ti: task instance given by xcom
    :return:
    '''
    ti = kwargs['ti']
    virtual_cluster_id = ti.xcom_pull(task_ids='get_emr_virtual_cluster_id', key='return_value')['virtual_cluster_id']
    args = 'aws emr-containers delete-virtual-cluster --id'.split()
    args.extend([virtual_cluster_id])
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)


def run_job_func(**kwargs):
    '''
    :param kwargs:
        ti: task instance given by xcom
        job_run_id : string type number like '1', '2', '3'
    :return:
    '''
    # XCom에서 값을 가져옴
    virtual_cluster_id = kwargs['ti'].xcom_pull(task_ids='get_emr_virtual_cluster_id', key='return_value')[
        'virtual_cluster_id']
    job_run_id = kwargs['job_run_id']
    args = f'aws emr-containers start-job-run --cli-input-json file:///opt/airflow/config/job-run-{job_run_id}.json'.split()
    args.extend(["--virtual-cluster-id", virtual_cluster_id])
    print(" ".join(args))
    result = subprocess.run(args=args, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
        data = json.loads(result.stdout)
        job_id = data.get('id')
        return {'job_id': job_id}
    if result.stderr:
        print(result.stderr)


def wait_job_done(**kwargs):
    import time
    ti = kwargs['ti']
    virtual_cluster_id = ti.xcom_pull(task_ids='get_emr_virtual_cluster_id', key='return_value')[
        'virtual_cluster_id']
    task_ids = kwargs['task_ids']
    job_id = kwargs['ti'].xcom_pull(task_ids=task_ids, key='return_value')['job_id']
    args = ['aws', 'emr-containers', 'describe-job-run', '--id', job_id, '--virtual-cluster-id', virtual_cluster_id]
    while True:
        result = subprocess.run(args=args, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout)
            data = json.loads(result.stdout)
            state = data.get('jobRun').get('state')
            if state == "FAILED" or state == "CANCELLED":
                print(state)
                print('HERE 1', state, state == "FAILED", state == "CANCELLED")
                return False
            elif state == "COMPLETED":
                createdAt = data.get('jobRun').get('createdAt')
                finishedAt = data.get('jobRun').get('finishedAt')
                ti.xcom_push(key=task_ids+"_"+createdAt, value=createdAt)
                ti.xcom_push(key=task_ids+"_"+finishedAt, value=finishedAt)
                return True
            else:
                time.sleep(60)
                continue
        if result.stderr:
            print(result.stderr)
            print('HERE 2')
            return False
