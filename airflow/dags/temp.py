import subprocess

from airflow.decorators import task_group, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.decorators import dag, task_group, task
from callable import *
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from airflow.operators.python import PythonOperator, ShortCircuitOperator

default_args = {
    'owner': 'airflow',
}

@dag(
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False,
    dag_id='temp',
    params= {'arr' : [1, 2]}
)
def task_group_mapping_example(arr):
    # creating a task group using the decorator with the dynamic input my_num
    @task_group(group_id="group1")
    def tg1(my_num):
        @task
        def print_num(num):
            return num

        print_num(my_num)
    # creating 6 mapped task group instances of the task group group1 (2.5 feature)
    arr = "{{ params['arr'] }}"
    tg1_object = tg1.expand(my_num=arr)

    # setting dependencies
    tg1_object


task_group_mapping_example()