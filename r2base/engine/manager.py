from sqlitedict import SqliteDict
import os
import datetime


class IndexManager(object):
    def __init__(self, index_dir):
        self.index_dir = index_dir

    @property
    def client(self):
        return SqliteDict(os.path.join(self.index_dir, 'db.sqlite'),
                          tablename='data',
                          autocommit=True)

    def put(self, index_id):
        self.client[index_id] = str(datetime.datetime.now())

    def get(self, index_id):
        return self.client.get(index_id)

    def pop(self, index_id):
        return self.client.pop(index_id, None)

    def list_indices(self):
        return list(self.client.keys())


if __name__ == '__main__':
    c = IndexManager('.')
    c.put('123')
    c.put('245')
    print(c.get('123'))
    c.pop('245')
    print(c.list_indices())