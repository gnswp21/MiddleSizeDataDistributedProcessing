from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime


# PythonOperator에서 실행할 함수
def task_in_group(task_name, **kwargs):
    print(f"Running {task_name}")


# Task Group 생성 함수
def create_task_group(group_number, dag):
    with TaskGroup(group_id=f"group_{group_number}") as group:
        # 첫 번째 Operator
        task1 = PythonOperator(
            task_id=f'task_{group_number}_1',
            python_callable=task_in_group,
            op_kwargs={'task_name': f'Task {group_number}_1'}
        )
        # 두 번째 Operator
        task2 = PythonOperator(
            task_id=f'task_{group_number}_2',
            python_callable=task_in_group,
            op_kwargs={'task_name': f'Task {group_number}_2'}
        )
        # 세 번째 Operator
        task3 = PythonOperator(
            task_id=f'task_{group_number}_3',
            python_callable=task_in_group,
            op_kwargs={'task_name': f'Task {group_number}_3'}
        )
    return group


# DAG 정의
with DAG(dag_id="dynamic_task_group_with_operators", start_date=datetime(2023, 1, 1), schedule_interval=None) as dag:
    # conf에서 task_group_count 값 가져오기 (기본값: 1)
    task_group_count = "{{ dag_run.conf['task_group_count'] | default(1) | int }}"
    # task_group_count에 따라 Task Group 생성 및 각 Task Group에 Operator 추가
    for i in range(1, int(task_group_count) + 1):
        create_task_group(i, dag)
