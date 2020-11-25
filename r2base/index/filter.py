from r2base.index import IndexBase
from r2base import IndexType as IT
from r2base import FieldType as FT
import logging
from typing import Dict, Union, List
import os
import sqlite3


class FilterIndex(IndexBase):
    type = IT.FILTER

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        self.fields = [key for key, mapping in self.mapping.items() if self._is_filter(mapping['type'])]
        self._client = None

    def _is_filter(self, field_type):
        return field_type in {FT.keyword, FT.float, FT.integer}

    @property
    def client(self):
        if self._client is None:
            self._client = sqlite3.connect(os.path.join(self.work_dir, 'db.sqlite'))
        return self._client

    def create_index(self):
        if not os.path.exists(self.work_dir):
            os.mkdir(self.work_dir)
            self.logger.info("Create data folder at {}".format(self.work_dir))

        schema = ['key TEXT']
        for field in self.fields:
            mapping = self.mapping[field]
            if mapping['type'] == FT.keyword:
                schema.append('{} TEXT'.format(field))
            elif mapping['type'] == FT.float:
                schema.append('{} REAL'.format(field))
            elif mapping['type'] == FT.integer:
                schema.append('{} INTEGER'.format(field))
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

    def select(self, query: str):
        c = self.client.cursor()
        query = 'SELECT * FROM data WHERE {}'.format(query)
        c.execute(query)
        res = c.fetchall()
        results = {row[0] for row in res}
        return list(results)


if __name__ == "__main__":
    root = '/Users/tonyzhao/Documents/projects/R2Base/_index'
    idx = 'kv-1'

    c = FilterIndex('.', 'test', {'f1': {'type': "keyword"}, 'f2': {"type": "integer"},
                                  'f3': {"type": "float"}})

    if not os.path.exists(os.path.join(c.work_dir, 'db.sqlite')):
        c.create_index()
        c.add({'f1': "haha", "f2": 10}, '123')
        c.add({'f1': "lala", "f3": 10.3}, '456')
        c.add({'f2': 12, "f3": 3.3}, '789')
        c.add([{'f2': 12, "f3": 5.3}, {'f2': 22, "f3": 1.1}],
              ['892', '555'])

    print(c.select('f2>1'))
