from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator # 이번 과정에 쓰이는 주 오퍼레이터
import random

with DAG(
    dag_id="dags_python_operator", # dag id 는 가급적 파이썬 파일명과 동일하게 
    schedule="30 6 * * *", # 매일 오전 6시 30분에 도는 Dag
    start_date=pendulum.datetime(2023, 9, 17, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', "AVOCADO"]
        rand_int = random.randint(0,3) # 0 ~ 3 중 임의의 값 리턴 
        print("fruit[rand_int] : ", fruit[rand_int])

    py_t1 = PythonOperator(
        task_id = "py_t1",
        python_callable = select_fruit # 어떠한 파이썬 함수를 사용할 것 이냐 
    )

    py_t1