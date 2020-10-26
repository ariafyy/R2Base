from r2base.index.keyvalue import KeyValueIndex, KeyValueRankIndex
from r2base.index.vector import VectorIndex
from r2base.engine.bases import EngineBase
import logging


class Ranker(EngineBase):

    logger = logging.getLogger(__name__)

    def __init__(self):
        pass

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



