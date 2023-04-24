from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.etl.sispro.dag8.logic_neonatal_mortality_rate_dag8 import NeonatalMortalityRateDag8

@log
class SisproNeonatalMortalityRateDag8(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        self.logger.debug(token)
        try:
            neonatal_mortality_rate_dag8 = NeonatalMortalityRateDag8()
            neonatal_mortality_rate_dag8.main_sispro(token)
            self.logger.info("Funciona: Dag 8 neonatal_mortality_rate_Dag8 token ")
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err

