from r2base.index.keyvalue import KeyValueIndex, KeyValueRankIndex
from r2base.index.vector import VectorIndex
import uuid
import logging
import pickle as pkl
import os
import numpy as np

class FT(object):
    id = '_id'
    keyword = 'keyword'
    vector = "vector"


class CompoundIndex(object):

    logger = logging.getLogger(__name__)

    def __init__(self):
        pass

    def _sub_index(self, index_id, field):
        return '{}-{}'.format(index_id, field)

    def _load_index(self, index_id):
        if not os.path.exists(index_id):
            raise Exception("Index {} does not exist.".format(index_id))

        _index = pkl.load(open(index_id, 'rb'))
        mappings = _index['mappings']
        return _index, mappings

    def _dump_index(self, index_id, _index):
        return pkl.dump(_index, open(index_id, 'wb'))

    def create_index(self, index_id, mappings):
        _index = {FT.id: KeyValueIndex(self._sub_index(index_id, FT.id)),
                  'mappings': mappings}

        # TODO: add eval order
        for field, mapping in mappings.items():
            sub_id = self._sub_index(index_id, FT.id)
            if field != FT.id:
                if mapping['type'] == FT.keyword:
                    _index[field] = KeyValueRankIndex(sub_id)
                elif mapping['type'] == FT.vector:
                    _index[field] = VectorIndex(sub_id, mapping['num_dim'])
                else:
                    _index[field] = None
            else:
                print("_id is reserved for internal indexing.")

        self._dump_index(index_id, _index)
        return True

    def add_doc(self, index_id, docs):
        if type(docs) is not list:
            docs = [docs]

        _index, mappings = self._load_index(index_id)

        ids = []
        for d in docs:
            if FT.id not in d:
                d[FT.id] = str(uuid.uuid4())

            ids.append(d[FT.id])
            _index[FT.id].set(d[FT.id], d)

            for field, value in d.items():
                if field == FT.id:
                    continue

                if field not in _index:
                    self.logger.warning("{} is not defined in mapping".format(field))
                    continue

                if type(_index[field]) is KeyValueRankIndex:
                    _index[field].add(value, d[FT.id])

                elif type(_index[field]) is VectorIndex:
                    _index[field].add(value, d[FT.id])

        self._dump_index(index_id, _index)
        return ids

    def read_doc(self, index_id, ids):
        if type(ids) is not list:
            ids = [ids]

        _index, mappings = self._load_index(index_id)
        docs = [_index[FT.id].get(_id) for _id in ids]
        return docs

    def query(self, index_id, q):
        """
        :param q: example query
        {
            query: {
                text: xxx,
                time: xxx,
                day: xxx,
                vec: {'vector': x, 'text': xxx}
            },
            "size": 10
        }
        :return:
        """
        q_body = q['query']
        top_k = q.get('size', 10)
        ranks = {}
        filters = set()
        _index, mappings = self._load_index(index_id)
        for field, value in q_body.items():
            if field == FT.id:
                filters = value if type(value) is list else [value]
                continue

            sub_index = _index[field]
            if type(sub_index) is KeyValueRankIndex:
                temp = sub_index.get(value)
                filters = filters.union(temp)

            if type(sub_index) is VectorIndex:
                temp = sub_index.rank(value, top_k)
                for score, _id in temp:
                    ranks[_id] = score + ranks.get(_id, 0.0)

        if len(filters) > 0:
            if len(ranks) > 0:
                filtered_ranks = [(k, v) for k, v in ranks.items() if k in filters]
                filtered_ranks = sorted(filtered_ranks, key=lambda x: x[1], reverse=True)
                docs = [{'_source': _index[FT.id].get(_id), 'score': s} for _id, s in filtered_ranks][0:top_k]
            else:
                docs = [{'_source': _index[FT.id].get(_id), 'score': -1} for _id in filters][0:top_k]
        else:
            if len(ranks) > 0:
                ranks = [(k, v) for k, v in ranks.items()]
                ranks = sorted(ranks, key=lambda x: x[1], reverse=True)
                docs = [{'_source': _index[FT.id].get(_id), 'score': s} for _id, s in ranks][0:top_k]
            else:
                # random sample some data
                docs = _index[FT.id].sample(top_k)
                docs = [{'_source': doc, 'score': -1} for doc in docs]

        return docs




