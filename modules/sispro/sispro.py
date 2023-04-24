import time
from selenium import webdriver
from selenium.webdriver.common.by import By
import random
import os


class sispro():
    def __init__(self, url, remote):
        self.options_chrome = webdriver.ChromeOptions()
        self.options_chrome.headless = True
        self.prefs = {'profile.default_content_settings.popups': 0,
                 'download.default_directory': '/home/seluser/Downloads'}
        self.options_chrome.add_experimental_option('prefs', self.prefs)
        self.driver = webdriver.Remote(
            command_executor=remote,
            options=self.options_chrome)
        self.sleep = random.randint(2, 10)
        self.wait = random.randint(4, 10)
        self.sleep_loads = random.randint(15, 30)
        time.sleep(self.sleep)
        self.driver.implicitly_wait(self.wait)
        self.driver.get(url)

    def table_browser(self, xpath, value):
        element = self.driver.find_element(By.XPATH, xpath)
        all_options = element.find_elements(By.TAG_NAME, "option")
        for option in all_options:
            if option.get_attribute("value") == value:
                option.click()
                break
        time.sleep(self.sleep)

    def click_action(self, xpath):
        options = self.driver.find_element(By.XPATH, xpath)
        options.click()
        time.sleep(self.sleep)

    def browser(self, list_xpath, list_values):
        for i in range(len(list_xpath)):
            self.table_browser(list_xpath[i], list_values[i])

    def end(self):
        self.driver.quit()

