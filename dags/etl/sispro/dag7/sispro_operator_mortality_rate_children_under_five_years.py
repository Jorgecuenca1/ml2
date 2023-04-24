from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from dags.etl.sispro.dag7.logic_mortality_rate_children_under_five_years_dag7 import \
    MortalityRateChildrenUnderFiveYearsDag7


@log
class SisproMortalityRateChildrenUnderFiveYearsDag7(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        self.logger.debug(token)
        try:
            mortality_rate_children_under_five_years = MortalityRateChildrenUnderFiveYearsDag7()
            mortality_rate_children_under_five_years.main_sispro(token)
            self.logger.info("Funciona: Dag 7 mortality_rate_in_children_under_5_years token ")
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err
