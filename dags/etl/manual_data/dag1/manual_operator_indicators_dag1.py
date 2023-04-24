from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.etl.manual_data.dag1.logic_manual_indicators import (ManualIndicatorsDag1)

@log
class ManualOperatorIndicatorsDag1(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        self.logger.debug(token)
        try:
            manual_indicators_dag1_token = ManualIndicatorsDag1()
            manual_indicators_dag1_token.main_manual_data(token)
            self.logger.info("Funciona: Dag1 manual_indicators_dag1")
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err
