from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from dag1_operator import (
    Dag1Operator
)

dag_id = "dag1"


def dummy_callable(action):
    return f"{datetime.now()}: {action} login and Dag1!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Requests for Login and Dag1",
            schedule_interval="*/10 * * * *",
            start_date=datetime(2023, 4, 23),
            catchup=False,
            is_paused_upon_creation=False
    ) as dag:
        start = PythonOperator(
            task_id="starting_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "starting"},
            dag=dag
        )

        dag1 = Dag1Operator(
            task_id="getting_and_dag1",
            dag=dag
        )

        finish = PythonOperator(
            task_id="finishing_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "finishing"},
            dag=dag
        )

        start >> dag1 >> finish

    return dag


globals()[dag_id] = create_dag(dag_id)
