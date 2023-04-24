import redis

from dags.dags_config import Config
from modules.log import log


@log
class RedisClient:
    def __init__(self):
        self.redis_client = redis.StrictRedis(
            **Config.REDIS_CONFIG
        )

    def __enter__(self):
        return self

    def override_existing_value_by_key(self, key, value):
        self.logger.info(f"Overriding existing value {value}")
        self.redis_client.delete(key)
        self.redis_client.set(key, value)

    def get_value_by_key(self, key):
        response = self.redis_client.get(key)
        response = response.decode("utf-8") if response else ""
        return response

    def delete_value_by_key(self, key):
        self.logger.info("Deleting value!")
        self.redis_client.lpop(key)

    def __exit__(self, type, value, traceback):
        client_id = self.redis_client.client_id()
        self.redis_client.client_kill_filter(
            _id=client_id
        )
