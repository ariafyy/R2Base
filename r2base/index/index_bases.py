from r2base.config import EnvVar
from r2base.utils import chunks
from typing import List, Union, Tuple, Dict
from r2base import FieldType as FT
from elasticsearch import Elasticsearch, RequestsHttpConnection
from elasticsearch import helpers as es_helpers
from r2base.mappings import parse_mapping, BasicMapping
import logging
import os


class IndexBase(object):
    type: str
    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str):
        self.root_dir = root_dir
        self.index_id = index_id
        self._mappings = None
        self.work_dir = os.path.join(self.root_dir, self.index_id)
        self._client = None

    def size(self) -> int:
        pass

    def create_index(self, *args, **kwargs) -> None:
        pass

    def delete_index(self, *args, **kwargs) -> None:
        pass

    def add(self, *args, **kwargs) -> None:
        pass

    def delete(self, *args, **kwargs) -> None:
        pass

    def read(self, *args, **kwargs):
        pass

    def update(self, *args, **kwargs):
        pass

    def rank(self, *args, **kwargs) -> List[Tuple[float, int]]:
        pass


class EsBaseIndex(IndexBase):
    def __init__(self, root_dir: str, index_id: str):
        super().__init__(root_dir, index_id)
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

    @classmethod
    def list_indexes(cls):
        es = Elasticsearch(
            hosts=[EnvVar.ES_URL],
            ca_certs=False,
            verify_certs=False,
            timeout=1000,
            connection_class=RequestsHttpConnection
        )
        res = es.indices.get_alias("*")
        return list(res.keys())

    @classmethod
    def _raw(cls, field: str):
        return field + '_raw'

    @classmethod
    def _deraw(cls, field: str):
        if field.endswith('_raw'):
            return field.replace('_raw', '')
        else:
            return field

    @classmethod
    def _deraw_src(cls, src):
        remove_field = []
        clean_src = {}

        for f, v in src.items():
            new_f = cls._deraw(f)
            if new_f != f:
                remove_field.append((f, new_f))
            else:
                clean_src[f] = v

        for f, new_f in remove_field:
            clean_src[new_f] = src[f]

        return clean_src

    def _make_index(self, index_id, config, params):
        if self.es.indices.exists(index=index_id):
            raise Exception("Index {} already existed".format(index_id))

        resp = self.es.indices.create(index=index_id, ignore=400, body=config, params=params)
        if resp.get('acknowledged', False) is False:
            self.logger.error(resp)
            raise Exception("Error in creating index")
        else:
            self.logger.info("Index {} is created".format(index_id))

    def _sql2json(self, sql_filter: str):
        sql_filter = 'SELECT * FROM "{}" WHERE {}'.format(self.index_id, sql_filter)
        res = self.es.sql.translate(body={'query': sql_filter})
        return res['query']

    def _dump_mapping(self, mappings: Dict):
        """
        Save the mapping to the disk
        :param mappings:
        :return:
        """
        return {k: v.dict() for k, v in mappings.items()}

    def _load_mapping(self, mappings: Dict):
        res = {}
        for key, m in mappings.items():
            temp = parse_mapping(m)
            if temp is None:
                raise Exception("Error in parsing mapping {}".format(key))
            res[key] = temp
        return res

    @property
    def mappings(self):
        try:
            if self._mappings is None:
                res = self.es.indices.get_mapping(index=self.index_id)
                self._mappings = self._load_mapping(res[self.index_id]['mappings']['_meta'])
            return self._mappings
        except Exception as e:
            self.logger.error(e)
            return {}

    def delete_index(self) -> None:
        try:
            return self.es.indices.delete(index=self.index_id)
        except Exception as e:
            self.logger.error(e)

    def delete(self, doc_ids: Union[List[str], str]) -> Dict:
        if type(doc_ids) is not list:
            doc_ids = [doc_ids]
        es_query = {'bool': {'should': [{'term': {FT.ID: _id}} for _id in doc_ids]}}
        res = self.es.delete_by_query(index=self.index_id, body={'query': es_query, 'size': len(doc_ids)})
        return res

    def delete_by_query(self, sql_filter: str):
        if sql_filter is None:
            raise Exception("Delete by query does not accept None filter")
        sql_filter = sql_filter.strip()

        if sql_filter == '*':
            json_filter = {'match_all': {}}
        else:
            json_filter = self._sql2json(sql_filter)

        res = self.es.delete_by_query(index=self.index_id, body={'query': json_filter})
        return res

    def read(self, doc_ids: Union[List[str], str]):
        if type(doc_ids) is not list:
            doc_ids = [doc_ids]
        es_query = {'bool': {'should': [{'term': {FT.ID: _id}} for _id in doc_ids]}}
        res = self.es.search(index=self.index_id, body={'query': es_query, 'size': len(doc_ids)})
        docs = []
        for h in res['hits']['hits']:
            src = h['_source']
            src = self._deraw_src(src)
            docs.append(src)
        return sorted(docs, key=lambda x:doc_ids.index(x['_uid']))

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

    def to_query_body(self, key, mapping, query, top_k, json_filter, from_):
        pass

    @classmethod
    def process_score(cls, mapping, score):
        return score


if __name__ == '__main__':
    a = {'a': 1, 'a_raw': 2, "b_raw": 3, 'c': 4}
    b = EsBaseIndex._deraw_src(a)
    print(b)

