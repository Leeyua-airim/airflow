from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator # 이번 과정에 쓰이는 주 오퍼레이터
import random 
from airflow.decorators import task

with DAG(
    dag_id="dags_python_task_decorator", # dag id 는 가급적 파이썬 파일명과 동일하게 
    schedule="0 2 * * 1", # 매주 월요일 오전 2시 
    start_date=pendulum.datetime(2023, 9, 17, tz="Asia/Seoul"),
    catchup=False,
    tags=['hello task'],
) as dag:
    
    @task(task_id = "python_task_1") #하단의 함수에 python_task_1 데커레이터 적용
    def print_context(some_input):
        print(some_input)

    # 함수 실행 자체를 task 로 설정
    python_task_1 = print_context("task_decorator 실행")
