from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.fetch_token.rest_fetch_token_client import RestFetchTokenClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


@log
class Dag1Operator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        print("ingreso")
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err
