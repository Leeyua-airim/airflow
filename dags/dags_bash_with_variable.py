from airflow import DAG
import pendulum
import datetime

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.models import Variable

with DAG(
    dag_id='dags_bash_with_variable',
    schedule='10 0 * * 6#2',
    start_date=pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    catchup=False
    ) as dag:
    
    # 외부에 전역변수 선언방식 -> 권고하지 않음 
    var_value = Variable.get('sample_key')

    bash_var_1 = BashOperator(
        task_id = "bash_var_1", 
        # 파이썬 변수화된 값을 출력
        bash_command = f'echo [파이썬접근]variable:{var_value}'
    )

    # 내부에서 처리하는 권고방식
    bash_var_2 = BashOperator(
        task_id = 'bash_var_2',
        # 전역변수에 바로 접근 
        bash_command="echo [바로접근]variable:{{var.value.sample_key}}"
    )

