from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.etl.sispro.dag5.logic_crude_birth_and_fertility_rate_dag5 import CrudeBirthAndFertilityRateDag5


@log
class SisproCrudeBirthAndFertilityRateDag5(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        self.logger.debug(token)
        try:
            crude_birth_and_fertility_rate_token = CrudeBirthAndFertilityRateDag5()
            crude_birth_and_fertility_rate_token.main_sispro(token)
            self.logger.info("Funciona: Dag 5 crude_birth_rate token ")
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err
