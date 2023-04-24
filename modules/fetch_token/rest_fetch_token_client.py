import json

import requests

from dags.dags_config import Config
from modules.log import log


@log
class RestFetchTokenClient:
    def __init__(self):
        self.login_url = Config.LOGIN_API_URL
        self.refresh_url = Config.REFRESH_API_URL
        self.rest_login_username = Config.LOGIN_USERNAME_APP
        self.rest_login_password = Config.LOGIN_PASSWORD_APP
        self.headers = {
            'Content-Type': 'application/json'
        }

    def __enter__(self):
        return self

    def login(self):
        payload = json.dumps({
            "username": self.rest_login_username,
            "password": self.rest_login_password
        })
        response = requests.request("POST", self.login_url, headers=self.headers, data=payload)
        return response.json()

    def refresh_token(self, refresh):
        payload = json.dumps({
            "refresh": refresh
        })
        response = requests.request("POST", self.refresh_url, headers=self.headers, data=payload)
        return response.json()
