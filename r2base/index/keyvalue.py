from r2base.index import IndexBase
from sqlitedict import SqliteDict
from r2base import IndexType as IT
import logging
from typing import Dict, Union, List
import os
import sqlite3
import numpy as np


class KVIndex(IndexBase):
    type = IT.LOOKUP
    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = SqliteDict(os.path.join(self.work_dir, 'db.sqlite'), autocommit=True)
        return self._client

    def create_index(self):
        if not os.path.exists(self.work_dir):
            os.mkdir(self.work_dir)
            self.logger.info("Create data folder at {}".format(self.work_dir))

    def size(self):
        return len(self.client)

    def set(self, key: str, value):
        if key is None:
            self.logger.warning("Try to save in redis with None")
            return None
        self.client[key] = value

    def get(self, key):
        if key is None:
            self.logger.warning("Try to get in redis with None")
            return None

        return self.client[key]

    def sample(self, size):
        random_ids = set(np.random.randint(0, len(self.client), size))
        res = []
        for key_id, key in enumerate(self.client.keys()):
            if key_id in random_ids:
                res.append(self.get(key))
        return res

    def delete(self, key):
        if key is None:
            self.logger.warning("Try to get in redis with None")
            return None

        return self.client.pop(key, None)


class FilterIndex(IndexBase):
    type = IT.FILTER

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = sqlite3.connect(os.path.join(self.work_dir, 'db.sqlite'))
        return self._client

    def create_index(self):
        if not os.path.exists(self.work_dir):
            os.mkdir(self.work_dir)
            self.logger.info("Create data folder at {}".format(self.work_dir))
        c = self.client.cursor()
        c.execute('CREATE TABLE IF NOT EXISTS data (key TEXT, value TEXT)')

    def add(self, key: Union[List[str], str], value: Union[List[str], str]):
        c = self.client.cursor()
        assert type(key) == type(value)
        if type(key) is str:
            c.execute('INSERT INTO data VALUES (?,?)', (key, value))
        else:
            for k, v in zip(key, value):
                c.execute('INSERT INTO data VALUES (?,?)', (k, v))

        return self.client.commit()

    def rank(self, key: str):
        c = self.client.cursor()
        c.execute('SELECT * FROM data WHERE key=?', (key,))
        res = c.fetchall()
        results = {v for k,v in res}
        return list(results)


if __name__ == "__main__":
    root = '/Users/tonyzhao/Documents/projects/R2Base/_index'
    idx = 'kv-1'
    if not os.path.exists(os.path.join(root, idx)):
        os.mkdir(os.path.join(root, idx))

    c = KVIndex('.', idx, {})
    c.create_index()
    c.set('1', '123')
    c.set('2', '456')
    c.set('3', '789')
    print(c.get('1'))
    print(c.sample(2))

    c = FilterIndex('.', 'test', {})
    c.create_index()
    c.add('o1', '123')
    c.add('o1', '456')
    c.add('o2', '789')
    print(c.rank('o1'))
