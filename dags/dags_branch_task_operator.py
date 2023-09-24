from airflow import DAG
import pendulum
import datetime

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_woth_branch_decorator',
    start_date = pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    schedule   = None,
    catchup    = False
) as dag:
    # 분기처리를 위한 데코레이터 
    @task.branch(task_id = 'python_branch_task')
    def select_random():
        import random # 필요한 라이브러리는 이렇게 내부에 할당 

        item_lst     = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        # 위 로직에서 A 가 선택시 'task_a' 를 반환 
        if selected_item == 'A':
            return 'task_a' # 실행되어야 하는 테스크 아이디값으로 반환되어야 함
        elif selected_item in ['B','C']: #선택된 값이 'B', C 인 경우 
            return ['task_b', 'task_c']

    # # 분기를 처리하는 메서드 branchpythonoperator
    # python_branch_task = BranchPythonOperator(
    #     task_id = 'python_branch_task',
    #     python_callable = select_random # python_callable 을 통해 매개변수 전달,
    # )

    def common_func(**kwargs):
        print(kwargs['selected'])

    task_a = PythonOperator(
        task_id = 'task_a',
        python_callable = common_func, # 위 함수 실행 
        op_kwargs={'selected' : 'A'}   # 이런식으로 값 전달 가능  
    )

    task_b = PythonOperator(
    task_id = 'task_b',
    python_callable = common_func, # 위 함수 실행 
    op_kwargs={'selected' : 'B'}   # 이런식으로 값 전달 가능  
    )   

    task_c = PythonOperator(
    task_id = 'task_c',
    python_callable = common_func, # 위 함수 실행 
    op_kwargs={'selected' : 'B'}   # 이런식으로 값 전달 가능  
    )

    # 태스크 실행 -> 세 테스크 후보 중 하나가 실행되어야 하므로, 다음과 같이 리스트로 묶어줌 
    select_random() >> [task_a, task_b, task_c]