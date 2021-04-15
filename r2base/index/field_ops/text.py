from r2base.index.index_bases import FieldOpBase
from r2base.mappings import TextMapping
from r2base.processors.pipeline import Pipeline
from typing import Dict, Optional
import logging


class TextField(FieldOpBase):
    logger = logging.getLogger(__name__)

    @classmethod
    def _use_tokens(cls, mapping: TextMapping):
        return 'tokenize' in mapping.processor

    @classmethod
    def process_value(cls, mapping: TextMapping, query: str, is_query: bool):
        kwargs = {'lang': mapping.lang, 'is_query': is_query}
        pipe = Pipeline(mapping.q_processor)
        value = pipe.run(query, **kwargs)
        return value

    @classmethod
    def to_mapping(cls, mapping: TextMapping):
        if cls._use_tokens(mapping):
            cls.logger.info("Detected customized tokenizer. Using cutter_analyzer")
            return {'type': 'text', "analyzer": "cutter_analyzer"}
        else:
            return {'type': 'text'}

    @classmethod
    def to_add_body(cls, mapping: TextMapping, value: str):
        pipe = Pipeline(mapping.processor)
        kwargs = {'lang': mapping.lang, 'is_query': False}
        return pipe.run(value, **kwargs)

    @classmethod
    def to_query_body(cls, key: str, mapping: TextMapping, query: str, top_k: int,
                      json_filter: Optional[Dict],
                      from_: int, highlight: Optional[Dict]):

        kwargs = {'lang': mapping.lang, 'is_query': True}

        if type(query) is dict:
            if json_filter is None:
                es_query = {
                    "_source": False,
                    "query": query,
                    "size": top_k,
                    "from": from_,
                }
            else:
                es_query = {
                    "_source": False,
                    "query": {"bool": {"must": query, "filter": json_filter}},
                    "size": top_k,
                    "from": from_
                }
        else:
            pipe = Pipeline(mapping.q_processor)
            value = pipe.run(query, **kwargs)
            if json_filter is None:
                es_query = {
                    "_source": False,
                    "query": {"match": {key: value}},
                    "size": top_k,
                    "from": from_
                }
            else:
                es_query = {
                    "_source": False,
                    "query": {"bool": {"must": {"match": {key: value}}, "filter": json_filter}},
                    "size": top_k,
                    "from": from_
                }
        if highlight:
            es_query["highlight"] = highlight
        print (es_query)
        return es_query
