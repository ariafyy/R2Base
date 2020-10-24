from enum import Enum
from r2base.index.keyvalue import KeyValueIndex, KeyValueRankIndex
import uuid
import logging


class FieldType(object):
    id = '_id'
    keyword = 'keyword'


class CompoundIndex(object):

    logger = logging.getLogger(__name__)

    def __init__(self):
        self._index = dict()
        self._kv_index = KeyValueIndex()
        self._kv_rank_index = KeyValueRankIndex()

    def _sub_index(self, index, field_id):
        return '{}-{}'.format(index, field_id)

    def _default_check(self, index):
        if index not in self._index:
            raise Exception("Index {} does not exist.".format(index))

        _index = self._index[index]
        mappings = _index['mappings']
        return _index, mappings

    def create_index(self, index, mappings):
        temp = {FieldType.id: self._kv_index, 'mappings': mappings}

        # TODO: add eval order
        for field, mapping in mappings.items():
            if field != FieldType.id:
                if mapping['type'] == FieldType.keyword:
                    temp[field] = self._kv_rank_index
                else:
                    temp[field] = None
            else:
                print("_id is reserved for internal indexing.")

        self._index[index] = temp
        return True

    def add_doc(self, index, docs):
        if type(docs) is not list:
            docs = [docs]

        _index, mappings = self._default_check(index)

        ids = []
        for d in docs:
            if FieldType.id not in d:
                d[FieldType.id] = str(uuid.uuid4())

            ids.append(d[FieldType.id])
            _index[FieldType.id].set(self._sub_index(index, FieldType.id), d[FieldType.id], d)

            for field, value in d.items():
                if field == FieldType.id:
                    continue

                if field not in mappings:
                    self.logger.warning("{} is not defined in mapping".format(field))
                    continue

                if type(_index[field]) is KeyValueRankIndex:
                    _index[field].add(self._sub_index(index, field), value, d['_id'])

        return ids

    def read_doc(self, index, ids):
        if type(ids) is not list:
            ids = [ids]

        _index, mappings = self._default_check(index)
        _id_index_obj = _index[FieldType.id]
        _sub_index = self._sub_index(index, FieldType.id)
        docs = [_id_index_obj.get(_sub_index, _id) for _id in ids]
        return docs


    def query(self, index, q):
        """
        :param q: example query
        {
            query: {
                text: xxx,
                time: xxx,
                day: xxx
            }
        }
        :return:
        """
        q_body = q['query']
        res = set()
        _index, mappings = self._default_check(index)
        for field, value in q_body.items():
            if field == FieldType.id:
                res = value if type(value) is list else [value]
                continue

            index_obj = _index[field]
            sub_index = self._sub_index(index, field)
            if index_obj is None:
                continue

            if type(index_obj) is KeyValueRankIndex:
                temp = index_obj.get(sub_index, value)
                res = res.union(temp)

        _id_index_obj = _index[FieldType.id]
        _sub_index = self._sub_index(index, FieldType.id)
        docs = [_id_index_obj.get(_sub_index, _id) for _id in res]
        return docs




