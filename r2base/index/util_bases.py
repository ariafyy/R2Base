from r2base.index import IndexBase
from r2base.config import EnvVar
from r2base.utils import chunks
from typing import List, Tuple, Union, Dict
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers as es_helpers


class EsBaseIndex(IndexBase):
    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        self.es = Elasticsearch(
            hosts=[EnvVar.ES_URL],
            ca_certs=False,
            verify_certs=False,
            timeout=1000,
            connection_class=RequestsHttpConnection
        )
        self.logger.info("Create ES client {}".format(EnvVar.ES_URL))

    def delete_index(self) -> None:
        try:
            return self.es.indices.delete(index=self.index_id)
        except Exception as e:
            self.logger.error(e)

    def delete(self, doc_ids: Union[List[int], int]) -> None:
        if type(doc_ids) is int:
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
                self.logger.info("Processing up to {}".format(c_id * chunk_size))
                res = es_helpers.bulk(self.es, actions=chunk, request_timeout=500)
            except Exception as e:
                self.logger.error(e)
                raise e

        self.logger.info("Done indexing to ES")
        return res