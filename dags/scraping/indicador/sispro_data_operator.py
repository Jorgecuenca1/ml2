from dags.dags_config import Config
from modules.redisclient.redis_client import RedisClient
from modules.manager.manager_files import ManagerFiles
from modules.sispro.sispro import sispro
from modules.minioclient.minio_client import MinioClient
from modules.log import log
from modules.retry import RetryOnException as retry
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
import time


@log
class DataSispro4Operator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        file = ManagerFiles('rb')
        minio = MinioClient()
        params = file.get_params(Config.PARAMETERS_API_URL, token)
        params_f, xpaths, values, clicks = file.filters_params_sispro(params, 'Ind4-M')
        try:
            self.logger.info("Starting scraping sispro4")
            sispro4 = sispro(params_f[0]['site']['url'], Config.SELENIUM_REMOTE)
            self.logger.info(sispro4.driver)
            sispro4.browser(xpaths, values)
            sispro4.click_action(clicks[0])
            time.sleep(20)
            sispro4.click_action(clicks[1])
            sispro4.click_action(clicks[2])
            time.sleep(15)
            sispro4.end()
            time.sleep(5)
            readfile = f"{values[0][values[0].rfind('/')+1:]}.csv"
            minio.put_object(bucket_name=Config.BUCKET_DATA, filename='staging/sispro_ts_bruta_mortalidad.csv', filepath=readfile)
        except Exception as err:
            self.logger.error(f"Exception with error: {err}")
            raise err

@log
class DataSispro5Operator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        file = ManagerFiles('rb')
        minio = MinioClient()
        params = file.get_params(Config.PARAMETERS_API_URL, token)
        params_f, xpaths, values, clicks = file.filters_params_sispro(params, 'Ind5')
        try:
            self.logger.info("Starting scraping sispro4")
            sispro4 = sispro(params_f[0]['site']['url'], Config.SELENIUM_REMOTE)
            sispro4.browser(xpaths, values)
            sispro4.click_action(clicks[0])
            time.sleep(20)
            sispro4.click_action(clicks[1])
            sispro4.click_action(clicks[2])
            time.sleep(15)
            sispro4.end()
            time.sleep(5)
            readfile = f"{values[0][values[0].rfind('/') + 1:]}.csv"
            minio.put_object(bucket_name=Config.BUCKET_DATA, filename='staging/sispro_ts_bruta_natalidad_fecundidad.csv',
                             filepath=readfile)
        except Exception as err:
            self.logger.error(f"Exception with error: {err}")
            raise err

@log
class DataSispro7Operator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        file = ManagerFiles('rb')
        minio = MinioClient()
        params = file.get_params(Config.PARAMETERS_API_URL, token)
        params_f, xpaths, values, clicks = file.filters_params_sispro(params, 'Ind7')
        try:
            self.logger.info("Starting scraping sispro4")
            sispro4 = sispro(params_f[0]['site']['url'], Config.SELENIUM_REMOTE)
            sispro4.browser(xpaths, values)
            sispro4.click_action(clicks[0])
            time.sleep(20)
            sispro4.click_action(clicks[1])
            sispro4.click_action(clicks[2])
            time.sleep(15)
            sispro4.end()
            time.sleep(5)
            readfile = f"{values[0][values[0].rfind('/') + 1:]}.csv"
            minio.put_object(bucket_name=Config.BUCKET_DATA, filename='staging/sispro_ts_morta_infantil.csv',
                             filepath=readfile)
        except Exception as err:
            self.logger.error(f"Exception with error: {err}")
            raise err

@log
class DataSispro8Operator(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @retry(5)
    def execute(self, context):
        redis = RedisClient()
        token = redis.get_value_by_key(Config.REDIS_TOKEN_ACCESS_KEY)
        file = ManagerFiles('rb')
        minio = MinioClient()
        params = file.get_params(Config.PARAMETERS_API_URL, token)
        params_f, xpaths, values, clicks = file.filters_params_sispro(params, 'Ind8')
        try:
            self.logger.info("Starting scraping sispro4")
            sispro4 = sispro(params_f[0]['site']['url'], Config.SELENIUM_REMOTE)
            sispro4.browser(xpaths, values)
            sispro4.click_action(clicks[0])
            time.sleep(20)
            sispro4.click_action(clicks[1])
            sispro4.click_action(clicks[2])
            time.sleep(15)
            sispro4.end()
            time.sleep(5)
            readfile = f"{Config.FOLDER_FILES}/{values[0][values[0].rfind('/') + 1:]}.csv"
            minio.put_object(bucket_name=Config.BUCKET_DATA, filename='staging/sispro_morta_men_cinco.csv',
                             filepath=readfile)
        except Exception as err:
            self.logger.error(f"Exception with error: {err}")
            raise err