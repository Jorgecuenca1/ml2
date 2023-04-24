from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from dags.etl.sispro.dag10.sispro_operator_deaths_different_causes import (
    SisproDeathsDifferentCausesDag10
)

dag_id = "sispro_defunciones_diferentes_causas_dag10"


def dummy_callable(action):
    return f"{datetime.now()}: {action} load deaths different causes for API!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Requests for load deaths different causes for API",
            schedule_interval="0 0 1 */6 *",
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

        sispro = SisproDeathsDifferentCausesDag10(
            task_id="deaths_different_causes_dag10",
            dag=dag
        )

        finish = PythonOperator(
            task_id="finishing_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "finishing"},
            dag=dag
        )

        start >> sispro >> finish

    return dag


globals()[dag_id] = create_dag(dag_id)
