from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults



class Dag1Operator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        print("ingreso")
        import os

        print("HOLA ACA INGRESO")

        os.remove("weather-dataset-rattle-package.zip")
        os.remove("weatherAUS.csv")
        os.system("kaggle datasets download -d jsphyg/weather-dataset-rattle-package --force")
        os.system("unzip weather-dataset-rattle-package.zip")
        # os.system("psql -h 130.211.118.80 -d postgres -U postgres -c \"\copy weather_1 FROM '~/weatherAUS.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',')\" -W ml2password")
        os.system("python3 model.py")
        os.system("python3 connection.py")