from r2base.index import IndexBase
from sqlitedict import SqliteDict
from r2base import IndexType as IT
import logging
from typing import Dict, Union, List, Any
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

    def create_index(self) -> None:
        if not os.path.exists(self.work_dir):
            os.mkdir(self.work_dir)
            self.logger.info("Create data folder at {}".format(self.work_dir))

    def delete_index(self) -> None:
        try:
            if self._client is not None:
                self.client.close()
                self._client = None
        except Exception as e:
            self.logger.error(e)

        try:
            os.remove(os.path.join(self.work_dir, 'db.sqlite'))
            os.removedirs(self.work_dir)
        except Exception as e:
            self.logger.error(e)

    def size(self) -> int:
        return len(self.client)

    def set(self, key: Union[List[int], int], value) -> None:
        if key is None:
            self.logger.warning("Try to save in redis with None")
            return None
        if type(key) is int:
            self.client[key] = value
        else:
            for k, v in zip(key, value):
                self.client[k] = v

    def get(self, key: int) -> Union[None, Dict]:
        if key is None:
            self.logger.warning("KV does not support None key")
            return None

        return self.client.get(key, None)

    def sample(self, size: int) -> List:
        random_ids = set(np.random.randint(0, len(self.client), size))
        while len(random_ids) < size:
            r = np.random.randint(0, len(self.client))
            if r not in random_ids:
                random_ids.add(r)
        res = []
        for key_id, key in enumerate(self.client.keys()):
            if key_id in random_ids:
                res.append(self.get(key))
        return res

    def delete(self, key: Union[List[int], int]) -> Any:
        if key is None:
            return None

        if type(key) is int:
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
    c.set(1, '123')
    c.set(2, '456')
    c.set(3, '789')
    print(c.size())
    c.delete(4)
    print(c.size())
    print(c.get(1))
    print(c.sample(2))
