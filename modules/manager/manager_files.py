import requests as rq
import os
from datetime import datetime
from modules.log import log
import json


@log
class ManagerFiles:
    def __init__(self, mode):
        self.mode = mode
        self.logger.info("Management files")
        self.name_file = ""

    def std_name(self, origin, name, dataType):
        name_file = f"{origin}_{name}_{datetime.today().strftime('%Y%m%d')}.{dataType}"
        self.logger.info(f"{name_file.lower()}")
        self.name_file = name_file.lower()

    def save_file(self, file):
        try:
            with open(self.name_file, self.mode) as f:
                f.write(file)
                f.close()
            self.logger.info("Process save is success")
        except Exception as E:
            self.logger.error(f"Error while saving file {E}")

    def get_params(self, endpoint, access):
        headers = {'Authorization': 'Bearer ' + access}
        try:
            response = rq.get(endpoint, headers=headers)
            data = json.loads(response.content.decode('utf-8'))
            self.logger.info("api_get ok")
            return data
        except Exception as E:
            self.logger.error(f'Error while getting params. More details: {E}')

    def open_any_file(self, path_file):
        file = open(path_file, self.mode)
        return file

    def filters_params_sispro(self, params, value):
        list_xpath = []
        list_values = []
        list_click = []
        valid_params = []
        for param in params:
            if param['object_filter'] == value:
                valid_params.append(param)
                list_xpath.append(param['name'])
                list_values.append(param['value'])
                list_click.append(param['tag'])
        self.logger.info(f"Valid: {len(valid_params)}")
        return valid_params, list_xpath, list_values, list_click
