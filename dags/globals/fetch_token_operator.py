from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.fetch_token.rest_fetch_token_client import RestFetchTokenClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


@log
class FetchTokenOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        rest_client = RestFetchTokenClient()

        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)  # only tests temp
        self.logger.info(token)  # only tests temp
        try:
            tokens = rest_client.login()
            redis.override_existing_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY, tokens['access'])
            redis.override_existing_value_by_key(Config.REDIS_TOKEN_REFRESH_KEY, tokens['refresh'])
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err
