from r2base.index.util_bases import FieldOpBase
from r2base.mappings import VectorMapping
from typing import List


class VectorField(FieldOpBase):

    @classmethod
    def to_mapping(cls, mapping: VectorMapping):
        return {'type': 'dense_vector', 'dims': mapping.num_dim}

    @classmethod
    def to_add_body(cls, mapping: VectorMapping, vector: List[float]):
        assert len(vector) == mapping.num_dim
        return vector

    @classmethod
    def to_query_body(cls, key: str, mapping: VectorMapping, vector: List[float], top_k: int):
        assert len(vector) == mapping.num_dim
        query = {
            "_source": False,
            'query': {
                "script_score": {
                    'query': {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, '{}') + 1.0".format(key),
                        "params": {
                            "query_vector": vector
                        }
                    }}},
            "size": top_k
        }
        return query

    @classmethod
    def hits2ranks(cls, mapping: VectorMapping, res):
        ranks = []
        for h in res['hits']['hits']:
            score = h.get('_score', 0.0) if h['_score'] else 0.0
            ranks.append((score, int(h['_id'])))
        return ranks
