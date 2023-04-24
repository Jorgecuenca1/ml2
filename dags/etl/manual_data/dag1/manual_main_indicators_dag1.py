from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from dags.etl.manual_data.dag1.manual_operator_indicators_dag1 import (
    ManualOperatorIndicatorsDag1
)

dag_id = "manual_indicators_dag1"


def dummy_callable(action):
    return f"{datetime.now()}: {action} burden manual indicators for API!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Requests for burden manual indicators for API",
            schedule_interval="0 0 1 */3 *",
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

        manual = ManualOperatorIndicatorsDag1(
            task_id="manual_indicators_dag1",
            dag=dag
        )

        finish = PythonOperator(
            task_id="finishing_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "finishing"},
            dag=dag
        )

        start >> manual >> finish

    return dag


globals()[dag_id] = create_dag(dag_id)
