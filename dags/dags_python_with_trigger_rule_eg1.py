from airflow import DAG
import pendulum
import datetime

from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
with DAG(
    dag_id='dags_python_with_trigger_rule_eg1',
    start_date = pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    schedule   = None,
    catchup    = False
) as dag:
    # bashoperator 
    bash_upstream_1  = BashOperator(
        task_id      = 'bash_upstram_1',
        bash_command = 'echo upstream1'
    )

    # 
    @task(task_id='python_upstream_1')
    def python_upstream_1(): 
        # 강제로 에러를 일으키도록 합니다. 
        raise AirflowException('[System!!!] downstream_1 Exception!')
    
    @task(task_id = 'python_upstream_2')
    def python_upstream_2():
        print("정상처리 ")
    
    @task(task_id = 'python_downstream_1', trigger_rule = 'all_done')
    def python_downstream_1():
        print("정상 처리")

    [bash_upstream_1, python_upstream_1(), python_upstream_2()] >> python_downstream_1()
