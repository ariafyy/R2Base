import numpy as np
from r2base.index import IndexBase
from r2base import IndexType as IT
import redis
import pickle as pkl
import logging
from typing import Dict


class RedisKVIndex(IndexBase):
    type = IT.KEYWORD
    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)

    @property
    def client(self):
        if self._client is None:
            self._client = redis.StrictRedis(host='localhost', port=6379, db=0)
        return self._client

    def set(self, key: str, value):
        if key is None:
            self.logger.warning("Try to save in redis with None")
            return None
        try:
            encoding = pkl.dumps(value)
        except Exception as e:
            self.logger.error(e)
            raise e
        return self.client.set(key, encoding)

    def get(self, key):
        if key is None:
            self.logger.warning("Try to get in redis with None")
            return None

        value = self.client.get(key)
        try:
            return pkl.loads(value)
        except Exception as e:
            self.logger.error(e)
            self.client.delete(key)
            raise Exception("Server Cache Error")

    def sample(self, size):
        res = []
        for i in range(size):
            key = self.client.randomkey()
            res.append(self.get(key.decode('utf8')))
        return res


class RedisKVRankIndex(IndexBase):
    type = IT.KEYWORD

    @property
    def client(self):
        if self._client is None:
            self._client = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
        return self._client

    def add(self, key:str, value:str):
        return self.client.sadd(key, *{value})

    def rank(self, key: str):
        res = self.client.sunion([key])
        return res


if __name__ == "__main__":

    c = RedisKVIndex('.', 'test', {}, make_dir=False)
    c.set('1', '123')
    c.set('2', '456')
    c.set('3', '789')
    print(c.get('1'))
    print(c.sample(2))
    exit()
    c = RedisKVRankIndex('.', 'test', {})
    c.add('o1', '123')
    c.add('o1', '456')
    c.add('o2', '789')
    print(c.rank('o1'))
