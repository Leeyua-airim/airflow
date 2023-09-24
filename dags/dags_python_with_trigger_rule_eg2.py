from airflow import DAG
import pendulum
import datetime

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_python_with_trigger_rule_eg2',
    start_date = pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    schedule   = None,
    catchup    = False
) as dag:
    # 분기처리를 위한 데코레이터 / 래핑
    @task.branch(task_id = 'branching')
    def random_branch():
        import random # 필요한 라이브러리는 이렇게 내부에 할당 

        item_lst     = ['A', 'B', 'C']
        selected_item = random.choice(item_lst)
        # 위 로직에서 A 가 선택시 'task_a' 를 반환 
        if selected_item == 'A':
            return 'task_a' # 실행되어야 하는 테스크 아이디값으로 반환되어야 함
        elif selected_item == 'B':
            return 'task_b'
        elif selected_item == 'C':
            return 'task_c'

    # # 분기를 처리하는 메서드 branchpythonoperator
    # python_branch_task = BranchPythonOperator(
    #     task_id = 'python_branch_task',
    #     python_callable = select_random # python_callable 을 통해 매개변수 전달,
    # )

    task_a = BashOperator(
        task_id = 'task_a',
        bash_command='echo upstream1'
    )

    @task(task_id = 'task_b')
    def task_b():
        print('정상 처리')

    @task(task_id = 'task_c')
    def task_c():
        print("정상 처리")
    
    @task(task_id = 'task_d', trigger_rule = 'none_skipped') # skip 이 없을때 돈다.
    def task_d():
        print('정상 처리')


    random_branch() >> [task_a, task_b(), task_c()] >> task_d()