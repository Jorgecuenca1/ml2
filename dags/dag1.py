from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow.operators.bash_operator import BashOperator
from dag1_operator import (
    Dag1Operator
)

dag_id = "dag1"
import os


def dummy_callable(action):
    return f"{datetime.now()}: {action} Dag1!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Requests for Login and Dag1",
            schedule_interval="*/2 * * * *",
            start_date=datetime(2023, 4, 24),
            catchup=False,
            is_paused_upon_creation=False
    ) as dag:
        start = PythonOperator(
            task_id="starting_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "starting"},
            dag=dag
        )

        t1 = Dag1Operator(
            task_id='train_models',
            dag=dag,
        )

        finish = PythonOperator(
            task_id="finishing_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "finishing"},
            dag=dag
        )

        start >> t1 >> finish

    return dag


globals()[dag_id] = create_dag(dag_id)

