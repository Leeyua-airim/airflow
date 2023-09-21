from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator


with DAG(
    dag_id='dags_bash_with_template',
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 9,1,tz="Asia/Seoul"),
    catchup=False,
) as dag:
    bash_t1 = BashOperator(
        task_id = "bash_t1",
        # jinja 템플릿 활용
        bash_command='echo "data_interval_end : {{ data_interval_end }}"'
    )

    bash_t2 = BashOperator(
        task_id = "bash_t2",
        # env 에서
        # The DAG run’s logical date as YYYY-MM-DD. Same as {{ dag_run.logical_date | ds }}
        # 변수를 딕셔너리로 저장해주는 느낌쓰
        env={
            'START_DATE' : '{{data_interval_start | ds}}', 
            'END_DATE' : '{{data_interval_end | ds }}',
        },
        # && 는 쉘 스크립트의 문법으로, 앞에것이 성공하면 뒤에것을 실행
        bash_command = 'echo $START_DATE && echo $END_DATE'
    )

    bash_t1 >> bash_t2
