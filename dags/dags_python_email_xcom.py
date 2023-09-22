from airflow import DAG
import pendulum
import datetime

from airflow.decorators import task
from airflow.operators.email import EmailOperator

with DAG(
    dag_id='dags_python_eamil_xcom',
    schedule='10 0 * * 6#2',
    start_date=pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    catchup=False
    ) as dag:

    
    @task(task_id = 'something_task')
    def some_logic(**kwargs):
        from random import choice
        return choice(['Succes', 'Fail'])
    

    send_email = EmailOperator(
        task_id = 'send_email',
        to = 'jaehwa.lee@codestates.com', #
        subject='[ {{ data_interval_end.in_timezone("Asia/Seoul") | ds }} ] some_logic 처리결과',
        html_content='{{ data_interval_end.in_timezone("Asia/Seoul") | ds }} 처리 결과는 <br> \
            {{ ti.xcom_pull(task_ids="something_task")}} 했습니다. <br>'
    )

    some_logic() >> send_email