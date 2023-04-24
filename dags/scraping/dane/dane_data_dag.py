from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from dags.scraping.dane.dane_data_operator import (
    DataDaneOperator
)

dag_id = "dane_data_dag"


def dummy_callable(action):
    return f"{datetime.now()}: {action} scraping over website dane!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Get data from website dane using scraping",
            schedule_interval="0 0 1 * *",
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

        data_dane_scraping = DataDaneOperator(
            task_id="dane_data_dag_task",
            dag=dag
        )

        finish = PythonOperator(
            task_id="finishing_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "finishing"},
            dag=dag
        )

        start >> data_dane_scraping >> finish

    return dag


globals()[dag_id] = create_dag(dag_id)
