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
from modules.prepareData.prepare_date_dags import PrepareData

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
        data = PrepareData(Config.FOLDER_FILES)
        params = file.get_params(Config.PARAMETERS_API_URL, token)
        params_f, xpaths, values, clicks = file.filters_params_sispro(params, 'Ind4-P')
        try:
            self.logger.info("Starting scraping sispro_ts_bruta_mortalidad")
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
            data.rename_file(f"{values[0][values[0].rfind('/')+1:]}.csv", f"{params_f[0]['object_locator']}_P.csv")
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
        data = PrepareData(Config.FOLDER_FILES)
        params = file.get_params(Config.PARAMETERS_API_URL, token)
        params_f, xpaths, values, clicks = file.filters_params_sispro(params, 'Ind4-D')
        try:
            self.logger.info("Starting scraping sispro_ts_bruta_mortalidad")
            sispro4 = sispro(params_f[0]['site']['url'], Config.SELENIUM_REMOTE)
            sispro4.browser(xpaths, values)
            sispro4.click_action(clicks[0])
            time.sleep(20)
            sispro4.click_action(clicks[1])
            sispro4.click_action(clicks[2])
            time.sleep(15)
            sispro4.end()
            time.sleep(5)
            data.rename_file(f"{values[0][values[0].rfind('/') + 1:]}.csv",
                             f"{params_f[0]['object_locator']}_D.csv")
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
        data = PrepareData(Config.FOLDER_FILES)
        minio = MinioClient()
        params = file.get_params(Config.PARAMETERS_API_URL, token)
        params_f, xpaths, values, clicks = file.filters_params_sispro(params, 'Ind4-M')
        try:
            self.logger.info("Starting scraping sispro_ts_bruta_mortalidad")
            sispro4 = sispro(params_f[0]['site']['url'], Config.SELENIUM_REMOTE)
            sispro4.browser(xpaths, values)
            sispro4.click_action(clicks[0])
            time.sleep(20)
            sispro4.click_action(clicks[1])
            sispro4.click_action(clicks[2])
            time.sleep(15)
            sispro4.end()
            time.sleep(5)
            data.rename_file(f"{values[0][values[0].rfind('/') + 1:]}.csv",
                             f"{params_f[0]['object_locator']}_M.csv")
            data.source4(f"{params_f[0]['object_locator']}_P.csv", f"{params_f[0]['object_locator']}_D.csv", f"{params_f[0]['object_locator']}_M.csv")
            data.saveFiles(f"{params_f[0]['object_locator']}.xlsx")
            readfile = f"{Config.FOLDER_FILES}/{params_f[0]['object_locator']}.xlsx"
            minio.put_object(bucket_name=Config.BUCKET_DATA, filename=f"staging/{params_f[0]['object_locator']}.xlsx",
                             filepath=readfile)
            self.logger.info("Process success")
        except Exception as err:
            self.logger.error(f"Exception with error: {err}")
            raise err