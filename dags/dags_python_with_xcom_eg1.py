from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id='dags_python_with_xcom_eg1',
    schedule='10 0 * * 6#2',
    start_date=pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    catchup=False
    ) as dag:

    @task(task_id = 'python_xcom_push_task1')
    def xcom_push1(**kwargs): # 함수 내에서 ㅎ
        ti = kwargs['ti'] # ti 저장 객체 
        # key value 를 기반한 저장 
        ti.xcom_push(key = 'result1', value = 'value_1')
        ti.xcom_push(key = 'result2', value = [1,2,3])

    @task(task_id = 'python_xcom_push_task2')
    def xcom_push2(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='result1', value='value_2')
        ti.xcom_push(key='result2', value=[1,2,3,4])

    @task(task_id = 'python_xcom_pull_task')
    def xcom_pull(**kwargs):
        ti = kwargs['ti']
        
        # 값을 꺼내오기는 하나 어디서 꺼내오는지 표기가 없음 
        # dags 순서를 보니 xcom_push2 가 후속 순위이므로, 이 값에 할당된 값을 꺼내온다. 
        value1 = ti.xcom_pull(key='result1') # 값을 꺼내오는 것 
        # 마찬가지로 값을 꺼내오기는 하나 task_ids 를 설정하여 꺼내오도록 함 
        # [1,2,3] 리턴 예상 
        value2 = ti.xcom_pull(key='result2', task_ids ='python_xcom_push_task1')
        
        print("value1 : ", value1)
        print("value2 : ", value2)


    xcom_push1() >> xcom_push2() >> xcom_pull()

    



