from r2base.index import IndexBase
from r2base import IndexType as IT
from r2base import FieldType as FT
import json
from typing import Dict, Union, List
import os
import sqlite3


class FilterIndex(IndexBase):
    type = IT.FILTER

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        self.fields = list(mapping.keys())
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

        schema = ['_id TEXT']
        for field in self.fields:
            mapping = self.mapping[field]
            if mapping['type'] == FT.keyword:
                schema.append('{} TEXT'.format(field))
            elif mapping['type'] == FT.float:
                schema.append('{} REAL'.format(field))
            elif mapping['type'] == FT.integer:
                schema.append('{} INTEGER'.format(field))
            else:
                raise Exception("Unknown field type {}".format(json.dumps(mapping)))

        schema = 'CREATE TABLE IF NOT EXISTS data {}'.format('({})'.format(','.join(schema)))
        c = self.client.cursor()
        c.execute(schema)
        self.client.commit()

    def add(self, data: Union[List[Dict], Dict], doc_ids: Union[List[str], str]):
        c = self.client.cursor()
        sql = 'INSERT INTO data VALUES ({})'.format(','.join(['?']*(len(self.fields)+1)))

        if type(data) is dict:
            row = [doc_ids] + [data.get(f, None) for f in self.fields]
            c.execute(sql, tuple(row))
        else:
            for d, doc_id in zip(data, doc_ids):
                row = [doc_id] + [d.get(f, None) for f in self.fields]
                c.execute(sql, row)

        return self.client.commit()

    def delete(self, doc_ids: Union[List[str], str]):
        if type(doc_ids) is str:
            doc_ids = [doc_ids]
        query = 'DELETE FROM data where _id IN ({})'.format(','.join(doc_ids))
        c = self.client.cursor()
        c.execute(query)
        self.client.commit()

    def size(self) -> int:
        c = self.client.cursor()
        res = c.execute('SELECT COUNT(*) FROM data')
        return res.fetchone()[0]

    def select(self, query: str, valid_ids: Dict=None, size: int=10000):
        c = self.client.cursor()
        if valid_ids is None:
            query = 'SELECT * FROM data WHERE {}'.format(query)
        else:
            keys = list(valid_ids.keys())
            query = 'SELECT * FROM data WHERE {} AND _id IN ({})'.format(query, ','.join(keys))

        cursor = c.execute(query)
        results = set()
        for row in cursor.fetchmany(size):
            if valid_ids is None:
                results.add(row[0])
            else:
                if row[0] in valid_ids:
                    results.add(row[0])

        return results


if __name__ == "__main__":
    root = '/Users/tonyzhao/Documents/projects/R2Base/_index'
    idx = 'kv-1'

    c = FilterIndex('.', 'test', {'f1': {'type': "keyword"},
                                  'f2': {"type": "integer"},
                                  'f3': {"type": "float"}})

    if not os.path.exists(os.path.join(c.work_dir, 'db.sqlite')):
        c.create_index()
        c.add({'f1': "haha", "f2": 10}, '123')
        c.add({'f1': "lala", "f3": 10.3}, '456')
        c.add({'f2': 12, "f3": 3.3}, '789')
        c.add([{'f2': 12, "f3": 5.3}, {'f2': 22, "f3": 1.1}],
              ['892', '555'])

    print(c.size())
    print(c.select('f2>1'))
    c.delete(['123', '456'])
    print(c.size())
    print(c.select('f1="haha"'))
