from airflow import DAG
import pendulum
import datetime

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.operators.branch import BaseBranchOperator

with DAG(
    dag_id='dags_base_branch_decorator',
    start_date = pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    schedule   = None,
    catchup    = False
) as dag:
    # BaseBranchOpertor 를 사용한 분기처리의 경우 다음과 같이 상속을 처리해야함
    # class 명칭은 커스텀 가능 
    class CustomBranchOperator(BaseBranchOperator):
        
        # choose_branch() 함수 명칭은 그대로 써야함 
        # context 도 그대로 써야함
        # python 오퍼레이터 쓸 때 op_kwargs 에 들어갈 값들
        # 값을 넣는 용도가 아니라 기본적인 값들이 context 에 들어가있음
        def choose_branch(self, context):
            import random
            
            print("context : ", context)

            item_lst = ['A', 'B', 'C']
            selected_item = random.choice(item_lst)
            if selected_item == 'A':
                return 'task_a'
            elif selected_item in ['B','C']:
                return ['task_b','task_c']
            
    custom_branch_operator = CustomBranchOperator(task_id='python_branch_task')

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
    custom_branch_operator >> [task_a, task_b, task_c]