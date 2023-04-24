import json
import requests
from dags.dags_config import Config
from modules.log import log


@log
class ApiQueriesClient:

    def __init__(self):
        self.api_url = Config.API_URL
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': ''
        }

    def post_api(self, route_table,  payload, token):
        self.headers['Authorization'] = 'Bearer ' + token
        response = requests.request("POST", self.api_url + route_table, headers=self.headers, data=payload)
        self.logger.info(response)
        return response.json()

    def get_api(self, route_table, token):
        self.headers['Authorization'] = 'Bearer ' + token
        response = requests.request("GET", self.api_url + route_table, headers=self.headers)
        return response.json()

