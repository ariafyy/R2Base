from collections import defaultdict

class KeyValueIndex(object):
    def __init__(self):
        self._index = dict()

    def set(self, index, key, value):
        if index not in self._index:
            self._index[index] = dict()

        self._index[index][key] = value
        return True

    def get(self, index, key):
        if index not in self._index:
            raise Exception("Index {} does not exist".format(index))
        return self._index[index].get(key)


class KeyValueRankIndex(object):
    def __init__(self):
        self._index = dict()

    def add(self, index, key, value):
        if index not in self._index:
            self._index[index] = defaultdict(set)

        self._index[index][key].add(value)
        return True

    def get(self, index, key):
        if index not in self._index:
            raise Exception("Index {} does not exist".format(index))

        return self._index[index].get(key, [])