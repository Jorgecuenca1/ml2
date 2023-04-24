from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from dags.dags_config import Config
from dags.globals.fetch_token_operator import (
    FetchTokenOperator
)

dag_id = "fetch_token_api"


def dummy_callable(action):
    return f"{datetime.now()}: {action} login and fetch tokens for API!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Requests for Login and Fetch Tokens for API",
            schedule_interval="0 12 * * 3,6",
            start_date=datetime(2022, 4, 4),
            catchup=False,
            is_paused_upon_creation=False
    ) as dag:
        start = PythonOperator(
            task_id="starting_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "starting"},
            dag=dag
        )

        fetch_token = FetchTokenOperator(
            task_id="getting_and_fetching_tokens",
            dag=dag
        )

        finish = PythonOperator(
            task_id="finishing_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "finishing"},
            dag=dag
        )

        start >> fetch_token >> finish

    return dag


globals()[dag_id] = create_dag(dag_id)
