from r2base.index import IndexBase
from sqlitedict import SqliteDict
from r2base import IndexType as IT
from r2base.mappings import BasicMapping
import logging
from typing import Dict, Union, List, Any, Tuple
import os
import numpy as np


class KVIndex(IndexBase):
    type = IT.LOOKUP
    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str, mapping: BasicMapping):
        super().__init__(root_dir, index_id, mapping)
        # self._client = None

    @property
    def client(self):
        # if self._client is None:
        # return self._client
        return SqliteDict(os.path.join(self.work_dir, 'db.sqlite'),
                          tablename=self.index_id,
                          autocommit=False)

    def create_index(self) -> None:
        if not os.path.exists(self.work_dir):
            os.mkdir(self.work_dir)
            self.logger.info("Create data folder at {}".format(self.work_dir))

    def delete_index(self) -> None:
        """
        try:
            if self._client is not None:
                self.client.close()
                self._client = None
        except Exception as e:
            self.logger.error(e)
        """
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
        client = self.client
        if type(key) is not list:
            client[int(key)] = value
        else:
            for k, v in zip(key, value):
                client[int(k)] = v
        client.commit()

    def get(self, key: Union[List[int], int]) -> Union[List[Dict], Dict]:
        if key is None:
            self.logger.warning("KV does not support None key")
            return dict()
        client = self.client
        if type(key) is not list:
            return client.get(int(key), dict())
        else:
            return [client.get(int(k), dict()) for k in key]

    def sample(self, size: int, return_value: bool = True, sample_mode='fixed') -> List:
        if size == 0:
            return []

        client = self.client
        db_size = len(client)

        if db_size == 0:
            return []

        db_keys = client.keys()

        if size >= db_size:
            random_ids = list(range(db_size))
        else:
            # evenly sample in the whole database
            if sample_mode == 'fixed':
                step = db_size//size
                random_ids = set(np.arange(0, db_size, step).tolist()[0:size])

            elif sample_mode == 'random':
                random_ids = set(np.random.randint(0, len(client), size))
                attempts = 0  # in case dead loop in a case that is impossible
                while len(random_ids) < size and attempts < 1000:
                    r = np.random.randint(0, db_size)
                    attempts += 1
                    if r not in random_ids:
                        random_ids.add(r)
            else:
                raise Exception("Unknown sample mode {}".format(sample_mode))
        db_keys = list(db_keys)
        res = [db_keys[idx] for idx in random_ids]

        if return_value:
            res = self.get(res)

        return res

    def delete(self, key: Union[List[int], int]) -> Any:
        if key is None:
            return None
        client = self.client
        if type(key) is int:
            res = client.pop(int(key), None)
        else:
            res = [client.pop(int(k), None) for k in key]

        client.commit()
        return res

    def scroll(self, limit: int = 200, last_key: int = None) -> Tuple[List, int]:
        if limit > 10000:
            self.logger.warn("Very large limit={} detected".format(limit))
            limit = 10000

        client = self.client
        if last_key is None:
            GET_VALUES = 'SELECT key, value FROM "%s" ' \
                         'ORDER BY key ASC LIMIT "%s"' % (client.tablename, limit)
        else:
            GET_VALUES = 'SELECT key, value FROM "%s" ' \
                         'WHERE key > "%s"' \
                         'ORDER BY key ASC LIMIT "%s"' % (client.tablename, last_key, limit)
        res = []
        new_last_key = None
        for values in client.conn.select(GET_VALUES):
            res.append(client.decode(values[1]))
            new_last_key = values[0]

        if new_last_key is not None:
            new_last_key = int(new_last_key)

        return res, new_last_key


if __name__ == "__main__":
    idx = 'kv-1'
    c = KVIndex('.', idx, BasicMapping(type='_id'))
    c.delete_index()
    c.create_index()
    keys = list(range(100))
    vals = [str(x) for x in keys]
    c.set(keys, vals)
    for i in range(100):
        assert len(c.sample(i)) == i
    exit(1)

    import time
    print("Start paging")
    s_time = time.time()
    limit = 200
    counter = 0
    last_key = None
    while True:
        data, last_key = c.scroll(limit, last_key)
        # print(len(data))
        if len(data) == 0:
            print(last_key)
            break


    print(time.time() - s_time)
