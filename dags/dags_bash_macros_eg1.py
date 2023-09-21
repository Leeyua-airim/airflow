from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator

with DAG(
    dag_id='dags_bash_with_macro_eg1',
    schedule='10 0 L * *', #0시 10분 L 말일 
    start_date=pendulum.datetime(2023, 9, 1, tz="Asia/Seoul"),
    catchup=False
) as dag : 
    
    # 배쉬오퍼레이터 
    # START_DATE : 전월 말일, END_DATE : 1일전
    bash_task_1 = BashOperator(
        task_id = 'bash_task_1',
        
        # 환경셋팅
        # 템플릿
        # 현재 스케줄이 L 로 되어 있기에  
        # UTC -> 한국 타임존
        # 템플릿에서 꺼내 쓸때 
        env={'START_DATE' : '{{ data_interval_start.in_timezone("Asia/Seoul") | ds }}',
             
             'END_DATE' : '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=1)) | ds}}'
             },
        bash_command='echo "START_DATE : $START_DATE" && echo "END_DATE: $END_DATE"'
    )