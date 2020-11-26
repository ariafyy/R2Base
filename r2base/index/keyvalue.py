from r2base.index import IndexBase
from sqlitedict import SqliteDict
from r2base import IndexType as IT
import logging
from typing import Dict, Union, List
import os
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

    def set(self, key: Union[List[str], str], value):
        if key is None:
            self.logger.warning("Try to save in redis with None")
            return None
        if type(key) is str:
            self.client[key] = value
        else:
            for k, v in zip(key, value):
                self.client[k] = v

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

    def delete(self, key: Union[List[str], str]):
        if key is None:
            return None
        if type(key) is str:
            return self.client.pop(key, None)
        else:
            return [self.client.pop(k, None) for k in key]


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
    print(c.size())
    c.delete('3')
    print(c.size())
    print(c.get('1'))
    print(c.sample(2))
