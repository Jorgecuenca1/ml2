import requests as rq
from modules.log import log
from bs4 import BeautifulSoup as BfS
from fake_headers import Headers
from modules.manager.manager_files import ManagerFiles
from modules.minioclient.minio_client import MinioClient



@log
class ScrapingDane:
    def __init__(self):
        self.headers_customs = Headers(os="linux", headers=True).generate()
        self.session_connect = rq.Session()

    def find_tittle(self, html_parse, object_filter, object_locator, tag, value):
        url_return = None
        std_titles = None
        for item in html_parse.find_all(object_filter):
            titles = item.get(object_locator)
            if titles:
                std_titles = " ".join(titles.split())
            if std_titles:
                if std_titles.find(tag) != -1:
                    url_return = item.get(value)
                    self.logger.debug(url_return)
        if url_return:
            return url_return
        else:
            self.logger.info("Not found url in web site")

    def browser_page(self, url, param):
        object_filter = param['object_filter']
        value = param['value']
        tag = param['tag']
        object_locator = param['object_locator']
        self.logger.info(f"URL: {url}")
        self.logger.info(f"FIND : {tag}")
        url_return, href_valid, list_std = [], [], []
        try:
            response_web = self.session_connect.get(url, headers=self.headers_customs)
        except Exception as e:
            self.logger.error(f"Was get an error while connecting to url. Details{e}")
            return None
        if response_web.status_code != 200:
            self.logger.warning(f"Something wrong while getting responses: {response_web.status_code}")
            return None
        html_parse = BfS(response_web.text, 'html.parser')

        lambda_std_list = lambda list_all: list(map(lambda n: list_std.append(n) if n else None, list_all))
        lambda_none_remove = lambda remove_none: list(map(lambda a:
                                                          href_valid.append(a.get(value)) if a.get(value) else None,
                                                          remove_none))
        lambda_find = lambda elements, element_find: list(
            map(lambda a: url_return.append(a) if a.find(element_find) != -1 else None,
                elements))
        lambda_std_list(html_parse.find_all(object_filter))
        if object_locator:
            url_return.append(self.find_tittle(html_parse, object_filter, object_locator, tag, value))
        else:
            lambda_none_remove(list_std)
            lambda_find(href_valid, tag)
        if url_return:
            self.logger.info(url_return[0])
            return url_return[0]
        else:
            self.logger.error("Not found url in web site")

    def iterator_browser(self, params):
        new_url = ''
        for param in params:
            new_url = self.browser_page(f"{param['site']['url']}{new_url}", param)
        data = self.session_connect.get(f"{param['site']['url']}{new_url}").text
        files = new_url.split('/')[-1]
        name = files.split('.')[0]
        file_type = files.split('.')[-1]
        file = ManagerFiles('a+')
        self.logger.info(f"{name},{file_type}")
        file.std_name(params[0]['site']['name'], name, file_type)
        minio_conn = MinioClient()
        bucket_name = 'data-scraping-dags'
        if minio_conn.bucket_exists(bucket_name) == bucket_name:
            pass
        else:
            minio_conn.create_bucket(bucket_name)
        try:
            file.save_file(data)
            self.logger.info(f"{file.name_file}")
            minio_conn.put_object(bucket_name, file.name_file, file.name_file)
            self.logger.info("Save file successful")
        except:
            self.logger.error("Dont save file")
