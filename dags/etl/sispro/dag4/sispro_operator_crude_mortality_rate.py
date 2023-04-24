from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.etl.sispro.dag4.logic_crude_mortality_rate import (CrudeMortalityRateDag4)

@log
class SisproOperatorCrudeMortalityRateDag4(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        self.logger.debug(token)
        try:
            crude_mortality_rate_dag4_token = CrudeMortalityRateDag4()
            crude_mortality_rate_dag4_token.main_sispro(token)
            self.logger.info("Funciona: Dag4 sispro_ts_bruta_mortalidad_Dag4")
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err
