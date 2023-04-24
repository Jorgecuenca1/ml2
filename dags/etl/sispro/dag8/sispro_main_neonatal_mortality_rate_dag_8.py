from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from dags.etl.sispro.dag8.sispro_operator_neonatal_mortality_rate import (
    SisproNeonatalMortalityRateDag8
)

dag_id = "sispro_ts_mortalidad_neonatal_dag8"


def dummy_callable(action):
    return f"{datetime.now()}: {action} load neonatal mortality rate for API!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Requests for load neonatal mortality rate for API",
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

        sispro = SisproNeonatalMortalityRateDag8(
            task_id="neonatal_mortality_rate_dag8",
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
