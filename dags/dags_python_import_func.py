from airflow  import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
# from plugins.common.common_func import get_sftp # 아무 셋팅을 하지 않으면 여기서 경로를 띄움
# 파이썬 가상환경을 만들었을 때 AIRFLOW 라는 폴더를 기본 경로로 셋팅해두었기 때문
# 근데 여기 개발환경에서는 에러가 안나지만, airflow 는 plugins path 까지 기본 경로로 잡고있기 때문
# 우리 airflow 는 opt/airflow/plugins 까지 경로로 잡고 있음 
# 그래서 .env 파일을 생성하여 pathonpath 를 plugins 까지 잡도록 셋팅 
# 그러면 더 이상 common 을 잡아도 경고가 뜨지 않음 
# .env 는 깃에 올릴 필요 까지 없음 
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz='Asia/Seoul'),
    catchup=False,
) as dag:
    task_get_sftp = PythonOperator( # dag 명칭
        task_id = "task_get_sftp",
        python_callable=get_sftp
    )