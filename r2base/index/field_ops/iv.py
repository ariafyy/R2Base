from r2base.index.util_bases import FieldOpBase
from r2base.mappings import TermScoreMapping
from typing import Dict, List, Optional
import logging
import numpy as np


class InvertedField(FieldOpBase):
    logger = logging.getLogger(__name__)

    ALPHA = 10.0
    MAX_NUM_COPY = 100

    @classmethod
    def to_mapping(self, mapping: TermScoreMapping):
        if mapping.mode == 'float':
            return {"type": "rank_features"}

        elif mapping.mode == 'int':
            return {"type": "text", "index_options": "freqs",
                    "analyzer": "cutter_analyzer", 'similarity': "tf_alone"}
        else:
            raise Exception("Unknown term score mode={}".format(mapping.mode))

    @classmethod
    def to_add_body(cls, mapping: TermScoreMapping, ts: Dict):
        if mapping.mode == 'float':
            ts = {term: np.exp(s) - 1.0 for term, s in ts.items()}
            return ts

        elif mapping.mode == 'int':
            tokens = []
            for term, s in ts.items():
                tokens.extend([term] * min(cls.MAX_NUM_COPY, int(s * cls.ALPHA)))
            return '/'.join(tokens)
        else:
            raise Exception("Unknown term score mode={}".format(mapping.mode))

    @classmethod
    def to_query_body(cls, key: str, mapping: TermScoreMapping, tokens: List[str], top_k: int, json_filter: Optional[Dict]):
        if mapping.mode == 'float':
            main_query = [{'rank_feature': {'field': '{}.{}'.format(key, t),
                                            "log": {"scaling_factor": 1.0}
                                            }} for t in tokens]
            if json_filter is None:
                es_query = {
                    "_source": False,
                    "query": {"bool": {"should": main_query}},
                    "size": top_k
                }
            else:
                es_query = {
                    "_source": False,
                    "query": {"bool": {"should": main_query, "filter": json_filter}},
                    "size": top_k
                }
            return es_query
        elif mapping.mode == 'int':
            if json_filter is None:
                es_query = {
                    "_source": False,
                    "query": {"match": {key: '/'.join(tokens)}},
                    "size": top_k
                }
            else:
                value = '/'.join(tokens)
                es_query = {
                    "_source": False,
                    "query": {"bool": {"must": {"match": {key: value}}, "filter": json_filter}},
                    "size": top_k
                }
            return es_query
        else:
            raise Exception("Unknown term score mode={}".format(mapping.mode))

    @classmethod
    def process_score(cls, mapping: TermScoreMapping, score):
        if mapping.mode == 'int':
            score /= InvertedField.ALPHA
        return score