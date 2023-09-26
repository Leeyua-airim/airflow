from airflow import DAG
import pendulum
import datetime

from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator

with DAG(
    dag_id='dags_empty_with_edge_label',
    start_date = pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    schedule   = None,
    catchup    = False
) as dag:
    
    empty_1 = EmptyOperator(
        task_id = 'empty_1'
    )

    empty_2 = EmptyOperator(
        task_id = 'empty_2'
    )

    empty_1 >> Label('1과 2 사이') >> empty_2

    empty_3 = EmptyOperator(
        task_id = 'empty_3'
    )

    empty_4 = EmptyOperator(
        task_id = 'empty_4'
    )

    empty_5 = EmptyOperator(
        task_id = 'empty_5'
    )

    empty_6 = EmptyOperator(
        task_id = 'empty_6'
    )

empty_2 >> Label("Start Branch") >> [empty_3, empty_4, empty_5] >> Label("End Branch") >> empty_6