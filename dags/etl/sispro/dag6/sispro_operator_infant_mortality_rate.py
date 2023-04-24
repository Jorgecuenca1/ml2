from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.etl.sispro.dag6.logic_infant_mortality_rate_dag6 import InfantMortalityRateDag6

@log
class SisproInfantMortalityRateDag6(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        self.logger.debug(token)
        try:
            infant_mortality_rate_dag6_token = InfantMortalityRateDag6()
            infant_mortality_rate_dag6_token.main_sispro(token)
            self.logger.info("Funciona: Dag 6 infant_mortality_rate_Dag6 token ")
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err

