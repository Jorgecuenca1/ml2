from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from dags.etl.sispro.dag7.sispro_operator_mortality_rate_children_under_five_years import (
    SisproMortalityRateChildrenUnderFiveYearsDag7
)

dag_id = "sispro_mortalidad_menores_cinco_años_dag7"


def dummy_callable(action):
    return f"{datetime.now()}: {action} load mortality rate in children under 5 years for API!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Requests for load mortality rate in children under 5 years for API",
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

        sispro = SisproMortalityRateChildrenUnderFiveYearsDag7(
            task_id="mortality_rate_in_children_under_5_years_dag7",
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
