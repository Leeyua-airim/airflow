from airflow import DAG
import pendulum
import datetime

from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_task_group',
    start_date = pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    schedule   = None,
    catchup    = False
) as dag:
    # 내부 함수 
    def inner_func(**kwargs):
        # 키로 접근하여 그 값을 출력
        msg = kwargs.get('msg') or ''
        print(msg)
    
    # 데코레이터를 이용한 그루핑 
    @task_group(group_id='first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번째 그룹 입니다. '''

        @task(task_id = 'inner_function1')
        def inner_func1(**kwargs):
            print("첫 번째 TaskGroup 내 첫 번째 task 입니다. ")

        inner_function2 = PythonOperator(
            task_id = 'inner_function2',
            python_callable=inner_func, # 외부에서 정의한 함수 매핑 
            op_kwargs = 
            {
                'msg' : '첫 번째 TaskGroup 내 두 번째 task 입니다.'
            }
        )

        inner_func1() >> inner_function2

    group_1()