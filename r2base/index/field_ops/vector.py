from r2base.index.index_bases import FieldOpBase
from r2base.mappings import VectorMapping
from typing import List, Optional, Dict


class VectorField(FieldOpBase):

    @classmethod
    def to_mapping(cls, mapping: VectorMapping):
        return {'type': 'dense_vector', 'dims': mapping.num_dim}

    @classmethod
    def to_add_body(cls, mapping: VectorMapping, vector: List[float]):
        assert len(vector) == mapping.num_dim
        return vector

    @classmethod
    def to_query_body(cls, key: str, mapping: VectorMapping,
                      vector: List[float],
                      top_k: int,
                      json_filter: Optional[Dict]):

        assert len(vector) == mapping.num_dim
        if json_filter is None:
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
        else:
            query = {
                "_source": False,
                'query': {
                    "script_score": {
                        'query': {"bool": {"filter": json_filter}},
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
    def process_score(cls, mapping: VectorMapping, score):
        score = score/2.0 # range is from 0 to 2. We normalize to 0 to 1
        return score
