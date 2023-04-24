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
