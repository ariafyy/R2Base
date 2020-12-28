from r2base.index import IndexBase
from sqlitedict import SqliteDict
from r2base import IndexType as IT
from r2base.mappings import BasicMapping
import logging
from typing import Dict, Union, List, Any
import os
import numpy as np


class KVIndex(IndexBase):
    type = IT.LOOKUP
    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str, mapping: BasicMapping):
        super().__init__(root_dir, index_id, mapping)
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = SqliteDict(os.path.join(self.work_dir, 'db.sqlite'),
                                      tablename=self.index_id,
                                      autocommit=False)
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
            self.client[int(key)] = value
        else:
            for k, v in zip(key, value):
                self.client[int(k)] = v
        self.client.commit()

    def get(self, key: int) -> Union[None, Dict]:
        if key is None:
            self.logger.warning("KV does not support None key")
            return None

        return self.client.get(int(key), None)

    def sample(self, size: int, return_value: bool = True) -> List:
        if self.size() == 0:
            return []

        db_size = len(self.client)
        if size >= db_size:
            random_ids = list(range(db_size))
        else:
            random_ids = set(np.random.randint(0, len(self.client), size))
            attempts = 0 # in case dead loop in a case that is impossible
            while len(random_ids) < size and attempts < 1000:
                r = np.random.randint(0, db_size)
                attempts += 1
                if r not in random_ids:
                    random_ids.add(r)
        res = []
        for key_id, key in enumerate(self.client.keys()):
            if key_id in random_ids:
                if return_value:
                    res.append(self.get(int(key)))
                else:
                    res.append(int(key))
        return res

    def delete(self, key: Union[List[int], int]) -> Any:
        if key is None:
            return None

        if type(key) is int:
            res = self.client.pop(int(key), None)
        else:
            res = [self.client.pop(int(k), None) for k in key]

        self.client.commit()
        return res


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
    print(c.get(1))
    c.delete_index()
    c.create_index()
    c.set(1, '123')
    c.set(2, '456')
    c.set(3, '789')
    print(c.get(1))
