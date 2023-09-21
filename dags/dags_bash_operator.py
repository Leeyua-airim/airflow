from airflow import DAG
from airflow.operators.bash import BashOperator
import datetime
import pendulum # 시간과 관련된 라이브러리 

with DAG(
    dag_id='dags_bash_operator',
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2023, 9, 15, tz="Asia/Seoul"),
    catchup=False 
) as dag:
    bash_t1 = BashOperator(
        task_id = "bash_t1",
        bash_command="echo Hello airflow ~ ! ", #echo 
    )

    bash_t2 = BashOperator(
        task_id = "bash_t2",
        bash_command="echo $HOSTNAME",   # $HOSTNAME 출력 
    )

    bash_t1 >> bash_t2