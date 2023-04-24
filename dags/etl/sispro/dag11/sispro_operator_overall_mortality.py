from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.etl.sispro.dag11.logic_overall_mortality_dag11 import OverallMortalityRateDag11

@log
class SisproOverallMortalityRateDag11(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        self.logger.debug(token)
        try:
            overall_mortality_rate_dag11 = OverallMortalityRateDag11()
            overall_mortality_rate_dag11.main_sispro(token)
            self.logger.info("Funciona: Dag 11 overall_mortality_Dag11 token ")
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err

