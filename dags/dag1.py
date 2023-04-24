from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 24),
    'retries': 0,
}

dag = DAG(
    'my_dag',
    default_args=default_args,
    description='A DAG to run a bash script every 2 minutes',
    schedule_interval=timedelta(minutes=2),
    catchup=False
)

run_script = BashOperator(
    task_id='run_script',
    bash_command='../principal.sh',
    dag=dag
)