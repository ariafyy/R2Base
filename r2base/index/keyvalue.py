from collections import defaultdict
import numpy as np
from r2base.index import BaseIndex
from r2base.index import IndexType as IT


class KeyValueIndex(BaseIndex):
    type = IT.KEYWORD

    def __init__(self, index_id):
        self.index_id = index_id
        self._index = dict()

    def set(self, key, value):
        self._index[key] = value
        return True

    def get(self, key):
        return self._index.get(key)

    def keys(self):
        return list(self.keys())

    def sample(self, size):
        random_keys = np.random.randint(0, len(self._index), size)
        return [self._index[self.keys()[key_idx]] for key_idx in random_keys]


class KeyValueRankIndex(BaseIndex):
    type = IT.KEYWORD

    def __init__(self, index_id):
        self._index = defaultdict(set)
        self.index_id = index_id

    def add(self, key, value):
        self._index[key].add(value)
        return True

    def get(self, key):
        res = set()
        if type(key) is not list:
            key = [key]

        for k in key:
            res = res.union(self._index.get(k, set()))

        return res