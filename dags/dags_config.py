import os


class Config:
    API_URL = "https://api.observatoriodesaludvillavicencio.org"
    PROXY_WEBPAGE = "https://free-proxy-list.net/"

    TESTING_URL = "https://google.com"

    LOGIN_API_URL = os.getenv('LOGIN_API_URL', 'http://127.0.0.1:8000/api/v1/users/token/')
    REFRESH_API_URL = os.getenv('REFRESH_API_URL', 'http://127.0.0.1:8000/api/v1/users/token/refresh/')
    LOGIN_USERNAME_APP = os.getenv('LOGIN_USERNAME_APP', 'username')
    LOGIN_PASSWORD_APP = os.getenv('LOGIN_PASSWORD_APP', 'password')
    PARAMETERS_API_URL = os.getenv('PARAMETERS_API_URL', 'http://127.0.0.1:8000/api/v1/scraping/parameters/')
    SELENIUM_REMOTE = os.getenv("SELENIUM_REMOTE", 'http://osv_selenium_webdriver:4444')


    REDIS_HOST = os.getenv('REDIS_HOST', '127.0.0.1')
    REDIS_PORT = os.getenv('REDIS_PORT', '6379')

    REDIS_CONFIG = {
        "host": REDIS_HOST,
        "port": REDIS_PORT,
        "db": 0
    }

    REDIS_PROXIES_KEY = "proxies"

    REDIS_TOKEN_ACCESS_KEY = "token_access"
    REDIS_TOKEN_REFRESH_KEY = "token_refresh"

    MINIO_URL = os.getenv('MINIO_URL')
    MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY')
    MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY')

    MINIO_CONFIG = {
        "endpoint": MINIO_URL,
        "access_key": MINIO_ACCESS_KEY,
        "secret_key": MINIO_SECRET_KEY,
        "secure": True
    }

    FOLDER_FILES = "/home/seluser/Downloads"
    BUCKET_DATA = "data-scraping-dags"

    MAX_WORKERS = 50

    VALIDATOR_CONFIG = {
        "description_length": 10,
        "languages": [
            "en", "es"
        ]
    }
