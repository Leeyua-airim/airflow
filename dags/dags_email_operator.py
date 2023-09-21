from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator", # dag id 는 가급적 파이썬 파일명과 동일하게 
    schedule="0 8 1 * *", # 매월 1일 오전 8시, 현재일 9월 17일 이므로 자동실행은 되지 않고 다음달에 실행
    start_date=pendulum.datetime(2023, 9, 17, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id = 'send_email_task',
        to='brink0@naver.com', # 받는사람
        subject="[테스트] 본 메일은 Airflow 를 통해 발신된 메일입니다.", # 제목 
        html_content='AIRFLOW 작업이 완료되었습니다.' # 메일 내용 
    )