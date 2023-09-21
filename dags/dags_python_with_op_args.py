from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from common.common_func import regist

with DAG(
    dag_id='dags_python_with_op_args',
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 9,1,tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    regist_t1 = PythonOperator(
        task_id = "regist_t1",
        python_callable=regist,
        op_args=['이재화','남자','kr', 126] # *args 
    )

    regist_t1