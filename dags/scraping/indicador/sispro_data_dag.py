from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from dags.scraping.indicador.sispro_data_operator import (
    DataSispro4Operator, DataSispro5Operator, DataSispro7Operator, DataSispro8Operator
)

dag_id = "sispro"


def dummy_callable(action):
    return f"{datetime.now()}: {action} scraping over website sispro!"


def create_dag(id):
    with DAG(
            dag_id=id,
            description=f"Get data from website sispro using scraping",
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

        data_sispro4_scraping = DataSispro4Operator(
            task_id="sispro4_data_dag_task",
            dag=dag
        )

        data_sispro5_scraping = DataSispro5Operator(
            task_id="sispro5_data_dag_task",
            dag=dag
        )

        data_sispro7_scraping = DataSispro7Operator(
            task_id="sispro7_data_dag_task",
            dag=dag
        )

        data_sispro8_scraping = DataSispro8Operator(
            task_id="sispro8_data_dag_task",
            dag=dag
        )

        finish = PythonOperator(
            task_id="finishing_pipeline",
            python_callable=dummy_callable,
            op_kwargs={"action": "finishing"},
            dag=dag
        )

        start >> data_sispro4_scraping >> data_sispro5_scraping >> data_sispro7_scraping >> data_sispro8_scraping >> finish

    return dag


globals()[dag_id] = create_dag(dag_id)
