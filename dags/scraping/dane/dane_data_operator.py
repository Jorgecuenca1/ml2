from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.manager.manager_files import ManagerFiles
from modules.dane.scraping_dane import ScrapingDane
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


@log
class DataDaneOperator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        file = ManagerFiles('a+')
        params = file.get_params(Config.PARAMETERS_API_URL, token)
        try:
            self.logger.info("Starting scraping dane")
            dane = ScrapingDane()
            dane.iterator_browser(params)
        except Exception as err:
            self.logger.error(f"Exception: {err}")
            raise err
