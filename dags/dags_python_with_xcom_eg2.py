from airflow import DAG
import pendulum
import datetime
from airflow.decorators import task

with DAG(
    dag_id='dags_python_with_xcom_eg2',
    schedule='10 0 * * 6#2',
    start_date=pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    catchup=False
    ) as dag:
    
    @task(task_id = 'python_xcom_push_by_return')
    def xcom_push_result(**kwargs):
        return "Success"
    
    @task(task_id = 'python_xcom_pull_1')
    def xcom_pull_1(**kwargs):
        ti = kwargs['ti']

        # 위 함수의 리턴값을 땡겨와 value1 에 저장 
        value1 = ti.xcom_pull(task_ids = 'python_xcom_push_by_return')
        
        print(f'xcom_pull 메서드로 직접 찾은 값 리턴(success 가 나와야함) :{value1}')
    
    @task(task_id = 'python_xcom_pull_2')
    def xcom_pull_2(status, **kawrgs):
        print('함수 입력값으로 받은 값(status) : {status}')
    
    # success 값 리턴 
    python_xcom_push_by_return = xcom_push_result()
    # 해당 값을 status 변수로 활용 
    xcom_pull_2(python_xcom_push_by_return)

    # 이렇게도 돌아갈 수 있음 
    python_xcom_push_by_return >> xcom_pull_1()
    



