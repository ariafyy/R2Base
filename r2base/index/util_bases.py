from r2base.index import IndexBase
from r2base.config import EnvVar
from r2base.utils import chunks
from typing import List, Union
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers as es_helpers
import logging


class EsBaseIndex(IndexBase):
    def __init__(self, root_dir: str, index_id: str, mapping):
        super().__init__(root_dir, index_id, mapping)
        self.es = Elasticsearch(
            hosts=[EnvVar.ES_URL],
            ca_certs=False,
            verify_certs=False,
            timeout=1000,
            connection_class=RequestsHttpConnection
        )
        tracer = logging.getLogger('elasticsearch')
        tracer.setLevel(logging.WARNING)  # or desired level
        self.logger.info("Create ES client {}".format(EnvVar.ES_URL))

    def _make_index(self, index_id, config, params):
        if self.es.indices.exists(index=index_id):
            raise Exception("Index {} already existed".format(index_id))

        resp = self.es.indices.create(index=index_id, ignore=400, body=config, params=params)
        if resp.get('acknowledged', False) is False:
            self.logger.error(resp)
            raise Exception("Error in creating index")
        else:
            self.logger.info("Index {} is created".format(index_id))

    def delete_index(self) -> None:
        try:
            return self.es.indices.delete(index=self.index_id)
        except Exception as e:
            self.logger.error(e)

    def delete(self, doc_ids: Union[List[int], int]) -> None:
        if type(doc_ids) is not list:
            doc_ids = [doc_ids]

        for doc_id in doc_ids:
            self.es.delete(self.index_id, doc_id)

    def size(self) -> int:
        if not self.es.indices.exists(index=self.index_id):
            return 0

        return self.es.count(index=self.index_id)['count']

    def run_bulk(self, data, chunk_size):
        res = None
        for c_id, chunk in enumerate(chunks(data, win_len=chunk_size, stride_len=chunk_size)):
            try:
                res = es_helpers.bulk(self.es, actions=chunk, request_timeout=500)
            except Exception as e:
                self.logger.error(e)
                raise e

        return res



class FieldOpBase(object):
    def to_mapping(self, mapping):
        raise NotImplementedError

    def to_add_body(self, mapping, value):
        return value

    def to_query_body(self, key, mapping, query, top_k, json_filter):
        pass

    @classmethod
    def process_score(cls, mapping, score):
        return score



