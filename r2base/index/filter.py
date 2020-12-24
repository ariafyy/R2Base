from r2base.index import IndexBase
from r2base import IndexType as IT
from r2base import FieldType as FT
import json
from typing import Dict, Union, List, Set
import os
import sqlite3


class FilterIndex(IndexBase):
    type = IT.FILTER

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        field_path = os.path.join(self.work_dir, 'fields.json')
        if not os.path.exists(field_path):
            fields = list(mapping.keys())
            json.dump(fields, open(field_path, 'w'))

        self.fields = json.load(open(field_path, 'r'))
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

        schema = ['_id INTEGER']
        for field in self.fields:
            mapping = self.mapping[field]
            if mapping['type'] == FT.KEYWORD:
                schema.append('{} TEXT'.format(field))
            elif mapping['type'] == FT.FLOAT:
                schema.append('{} REAL'.format(field))
            elif mapping['type'] == FT.INT:
                schema.append('{} INTEGER'.format(field))
            elif mapping['type'] == FT.DATE:
                schema.append('{} DATE'.format(field))
            elif mapping['type'] == FT.DATETIME:
                schema.append('{} TIMESTAMP'.format(field))
            else:
                raise Exception("Unknown field type {}".format(json.dumps(mapping)))

        schema = 'CREATE TABLE IF NOT EXISTS data {}'.format('({})'.format(','.join(schema)))
        c = self.client.cursor()
        c.execute(schema)
        self.client.commit()

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

    def add(self, data: Union[List[Dict], Dict], doc_ids: Union[List[int], int]) -> None:
        c = self.client.cursor()
        sql = 'INSERT INTO data VALUES ({})'.format(','.join(['?'] * (len(self.fields) + 1)))

        if type(data) is dict:
            row = [doc_ids] + [data.get(f, None) for f in self.fields]
            c.execute(sql, tuple(row))
        else:
            for d, doc_id in zip(data, doc_ids):
                row = [doc_id] + [d.get(f, None) for f in self.fields]
                c.execute(sql, row)

        self.client.commit()

    def delete(self, doc_ids: Union[List[int], int]):
        if type(doc_ids) is int:
            doc_ids = [doc_ids]
        query = 'DELETE FROM data where _id IN ({})'.format(','.join('?' * len(doc_ids)))
        c = self.client.cursor()
        c.execute(query, doc_ids)
        self.client.commit()

    def size(self) -> int:
        c = self.client.cursor()
        res = c.execute('SELECT COUNT(*) FROM data')
        return res.fetchone()[0]

    def select(self, query: Union[str, None], valid_ids: List[int] = None, size: int = 10000) -> Set[int]:

        c = self.client.cursor()
        if valid_ids is None:
            if query == "" or query is None:
                query = 'SELECT * FROM data'
            else:
                query = 'SELECT * FROM data WHERE {}'.format(query.strip())

            cursor = c.execute(query)
        else:
            valid_ids = [int(x) for x in valid_ids]
            if query == "" or query is None:
                query = 'SELECT * FROM data WHERE _id IN ({})'.format(','.join(['?'] * len(valid_ids)))
            else:
                query = 'SELECT * FROM data WHERE {} AND _id IN ({})'.format(query.strip(), ','.join(['?'] * len(valid_ids)))

            cursor = c.execute(query, valid_ids)

        results = set()
        for row in cursor.fetchmany(size):
            if valid_ids is None:
                results.add(row[0])
            else:
                if row[0] in valid_ids:
                    results.add(row[0])

        return results


if __name__ == "__main__":
    mapping = {
        "date": {
            "type": "datetime"
        },
        "issuer": {
            "type": "keyword"
        },
        "oid": {
            "type": "keyword"
        },
        "url": {
            "type": "keyword"
        },
        "source": {
            "type": "keyword"
        }
    }
    c = FilterIndex('/Users/tonyzhao/Documents/projects/R2Base/_index/123',
                    index_id='123-filter',
                    mapping=mapping)
    query = "date=datetime(\"2020-10-26 12:23:00\")"
    ids = ['6976962558034708725', '6976576715352705269', '6977457853663283445', '6977264133458364661', '6976927296353208565', '6976997175471114485', '6977031062763079925', '6977064434658969845', '6977162192409594101', '6976723822277560565', '6977296942713538805', '6976689075992135925', '6977556878429260021', '6976381942880798965', '6977098429325117685', '6977196139831101685', '6976419180247255285', '6977230542519142645', '6977130083234089205', '6976460497832642805', '6977332178625235189', '6976825466973587701', '6976539289007687925', '6977427058747771125', '6976790892486854901', '6976498572717721845', '6977397616746957045', '6976613978488965365', '6977494683007846645', '6976858688545622261', '6976338370437581045', '6976892610197326069', '6976652117798553845', '6977525864470415605', '6977364631398123765']
    print(c.select(query, valid_ids=ids))

    exit()
    c = FilterIndex('.', 'test_filter_index',
                    {'f1': {'type': "keyword"},
                     'f2': {"type": "integer"},
                     'f3': {"type": "float"},
                     "f4": {'type': "date"},
                     "f5": {'type': "datetime"}})

    c.create_index()
    c.add({'f1': "haha", "f2": 10, "f4": '2019-06-28'}, 123)
    c.add({'f1': "lala", "f3": 10.3, "f5": '2020-06-28 20:57:32'}, 456)
    c.add({'f2': 12, "f3": 3.3}, 666)
    c.add([{'f2': 0, "f3": 5.3}, {'f2': 22, "f3": 1.1}],
          [789, 900])

    print(c.select('f4=date("2019-06-28")'))
    print(c.select('f5=datetime("2020-06-28 20:57:32")'))
    print(c.select('f4 BETWEEN date("2019-05-28") AND date("2019-07-28")'))
    assert c.size() == 5
    assert len(c.select('f2>1')) == 3
    c.delete([789, 666])
    assert c.size() == 3
    assert len(c.select('f1="haha"')) == 1
    assert len(c.select('f1="haha"', valid_ids=[123, 456])) == 1
    assert len(c.select('f1="haha"', valid_ids=[666, 456])) == 0
