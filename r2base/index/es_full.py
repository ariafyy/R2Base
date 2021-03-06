from r2base import IndexType as IT
from r2base import FieldType as FT
from r2base.index.index_bases import EsBaseIndex
from r2base.config import EnvVar
from r2base.index.field_ops.text import TextField
from r2base.index.field_ops.iv import InvertedField
from r2base.index.field_ops.vector import VectorField
from r2base.index.field_ops.filter import FilterField
from r2base.index.field_ops.object import ObjectField
from r2base.mappings import BasicMapping
from typing import List, Union, Dict, Optional


class EsIndex(EsBaseIndex):
    type = IT.RANK
    raw_mapping = BasicMapping.parse_obj({"type": FT.OBJECT})

    def __init__(self, root_dir: str, index_id: str):
        super().__init__(root_dir, index_id)
        self._default_src = None

    @classmethod
    def _get_field_op(cls, f_type):
        if f_type == FT.TEXT:
            return TextField

        elif f_type == FT.VECTOR:
            return VectorField

        elif f_type == FT.TERM_SCORE:
            return InvertedField

        elif f_type == FT.OBJECT:
            return ObjectField

        elif f_type in FT.FILTER_TYPES:
            return FilterField

        else:
            raise Exception("Unknown field type {}".format(f_type))

    @property
    def default_src(self):
        if self._default_src is None:
            temp = [FT.ID]
            temp_list = {x for x in FT.FILTER_TYPES}
            temp_list.add(FT.TEXT)
            temp_list.add(FT.OBJECT)

            for field, mapping in self.mappings.items():
                if mapping.type in temp_list:
                    if mapping.save_raw:
                        temp.append(self._raw(field))
                    temp.append(field)

            self._default_src = temp

        return self._default_src

    def create_index(self, mappings: Dict) -> None:
        if FT.ID in mappings.keys():
            raise Exception("{} is reserved. Index creation aborted".format(FT.ID))

        params = {"timeout": '60s'}
        setting = EnvVar.deepcopy(EnvVar.ES_SETTING)
        properties = {FT.ID: {'type': 'keyword'}}

        obj_mappings = self._load_mapping(mappings)
        obj_mappings[FT.ID] = BasicMapping(type=FT.ID)

        for field, m in obj_mappings.items():
            mapping: BasicMapping = m
            if mapping.type in {FT.META, FT.ID}:
                continue

            properties[field] = self._get_field_op(mapping.type).to_mapping(mapping)

            if mapping.save_raw:
                properties[self._raw(field)] = self._get_field_op(FT.OBJECT).to_mapping(self.raw_mapping)

        config = {
            'mappings': {'properties': properties, '_meta': self._dump_mapping(obj_mappings)},
            'settings': setting
        }

        self._make_index(self.index_id, config, params)

    def add(self, data: Union[List[Dict], Dict], doc_ids: Union[List[str], str]) -> None:
        if type(data) is dict:
            data = [data]
            doc_ids = [doc_ids]

        assert len(doc_ids) == len(data)
        index_data = []
        for doc, doc_id in zip(data, doc_ids):
            body = {}
            for field, value in doc.items():
                mapping: BasicMapping = self.mappings[field]
                if mapping.type in {FT.META, FT.ID}:
                    continue

                body[field] = self._get_field_op(mapping.type).to_add_body(mapping, value)
                if mapping.save_raw:
                    body[self._raw(field)] = self._get_field_op(FT.OBJECT).to_add_body(self.raw_mapping, value)

            body[FT.ID] = str(doc_id)
            body['_op_type'] = 'index'
            body['_index'] = self.index_id
            index_data.append(body)

        if len(index_data) > 0:
            self.run_bulk(index_data, 5000)
        else:
            self.logger.warn("Skip add since data is empty.")

    def _get_src_filters(self, includes: Optional[List],
                         excludes: Optional[List],
                         reduce_includes: Optional[List]):

        if type(includes) is list and len(includes) == 0:
            includes = None

        if type(excludes) is list and len(excludes) == 0:
            excludes = None

        if reduce_includes is None:
            reduce_includes = []

        # make sure ID is in the source
        if includes is not None and FT.ID not in includes:
            includes = includes + [FT.ID]

        # make sure ID is not excluded
        if excludes is not None and FT.ID in excludes:
            excludes = [e for e in excludes if e != FT.ID]

        # set up sources
        if includes is None and excludes is None:
            return self.default_src + reduce_includes

        elif includes is None and excludes is not None:
            return {'excludes': excludes}

        elif includes is not None and excludes is None:
            return {'includes': includes + reduce_includes}

        else:
            return {'includes': includes + reduce_includes, 'excludes': excludes}

    def _empty_query(self, json_filter, top_k, includes=None, excludes=None, reduce_includes=None, from_=0):
        src_filter = self._get_src_filters(includes, excludes, reduce_includes)

        if json_filter is None:
            es_query = {
                "_source": src_filter,
                "query": {"match_all": {}},
                "size": top_k,
                "from": from_
            }
        else:
            es_query = {
                "_source": src_filter,
                "query": {"bool": {"must": {"match_all": {}}, "filter": json_filter}},
                "size": top_k,
                "from": from_
            }

        res = self.es.search(body=es_query, index=self.index_id)
        docs = []
        for h in res['hits']['hits']:
            score = h.get('_score', 0.0) if h['_score'] else 0.0
            src = h['_source']
            src = self._deraw_src(src)
            docs.append({'score': score, '_source': src})

        return docs

    def _query(self, match: dict, json_filter, top_k: int, includes=None, excludes=None, reduce_includes=None, from_=0):
        es_qs = []
        keys = []
        ths = []
        src_filter = self._get_src_filters(includes, excludes, reduce_includes)

        for field, value in match.items():
            mapping = self.mappings[field]

            if type(value) is dict:
                threshold = value.get('threshold', None)
                value = value['value']
            else:
                threshold = None

            ths.append(threshold)

            if mapping.type in FT.MATCH_TYPES:
                es_query = self._get_field_op(mapping.type).to_query_body(field, mapping, value, top_k, json_filter, from_)
                es_query['_source'] = src_filter
                keys.append(field)
                es_qs.append({'index': self.index_id})
                es_qs.append(es_query)

        m_res = self.es.msearch(body=es_qs)
        id2score = {}
        id2src = {}
        for k_id, key in enumerate(keys):
            mapping = self.mappings[key]
            threshold = ths[k_id]

            res = m_res['responses'][k_id]
            for h in res['hits']['hits']:
                score = h.get('_score', 0.0) if h['_score'] else 0.0
                score = self._get_field_op(mapping.type).process_score(mapping, score)
                if threshold is not None and score < threshold:
                    continue

                src = h['_source']
                src = self._deraw_src(src)

                doc_id = src[FT.ID]
                id2src[doc_id] = src
                id2score[doc_id] = score + id2score.get(doc_id, 0.0)

        idrank = [(doc_id, score) for doc_id, score in id2score.items()]
        idrank = sorted(idrank, key=lambda x: x[1], reverse=True)[0:top_k]
        docs = []
        for doc_id, score in idrank:
            docs.append({"_source": id2src[doc_id], 'score': score})

        return docs

    def rank(self, match: Optional[Dict],
             sql_filter: Optional[str],
             top_k: int,
             includes: Optional[List] = None,
             excludes: Optional[List] = None,
             reduce_includes: Optional[List] = None, 
             from_: int = 0) -> List[Dict]:

        if sql_filter is not None and sql_filter:
            json_filter = self._sql2json(sql_filter)
        else:
            json_filter = None

        if match and len(match) > 0:
            docs = self._query(match, json_filter, top_k, includes, excludes, reduce_includes, from_)
        else:
            docs = self._empty_query(json_filter, top_k, includes, excludes, reduce_includes, from_)

        return docs

    def scroll(self, match: Optional[Dict], sql_filter: Optional[str], batch_size: int,
               adv_match: Optional[Dict] = None,
               includes: Optional[List] = None,
               excludes: Optional[List] = None,
               sort: Optional[List] = None,
               search_after: Optional[List] = None):

        # build source
        src_filter = self._get_src_filters(includes, excludes, None)

        if not sort:
            sort = {"_uid": "asc"}

        query = None
        if adv_match is not None and len(adv_match) > 0:
            query = adv_match

        elif match is not None:
            temp = []
            for key, value in match.items():
                mapping = self.mappings[key]
                if mapping.type == FT.TEXT and type(value) is str:
                    p_value = self._get_field_op(mapping.type).process_value(mapping, value, is_query=True)
                    temp.append({'match': {key: p_value}})

            if len(temp) > 0:
                query = {'bool': {'should': temp}}

        if query is None:
            query = {"match_all": {}}

        es_query = {
            "_source": src_filter,
            "size": batch_size,
            'sort': sort
        }

        if sql_filter is not None and sql_filter:
            json_filter = self._sql2json(sql_filter)
            es_query['query'] = {"bool": {"must": query, "filter": json_filter}}
        else:
            es_query['query'] = query

        if search_after:
            es_query['search_after'] = search_after

        res = self.es.search(body=es_query, index=self.index_id)
        docs = []
        last_id = None
        for h in res['hits']['hits']:
            src = h['_source']
            docs.append(self._deraw_src(src))
            last_id = h['sort']

        return docs, last_id


if __name__ == "__main__":
    import time
    from r2base.mappings import VectorMapping, TextMapping, TermScoreMapping, BasicMapping

    root = '.'
    idx = 'full_es_test'
    mapping = {
        'vector': VectorMapping(type=FT.VECTOR, num_dim=4),
        'ts': TermScoreMapping(type=FT.TERM_SCORE, mode='int'),
        'f_ts': TermScoreMapping(type=FT.TERM_SCORE, mode='float'),
        'text': TextMapping(type=FT.TEXT, lang='zh', index='bm25'),
        'kw': BasicMapping(type=FT.KEYWORD),
        'time': BasicMapping(type=FT.DATE),
        'int': BasicMapping(type=FT.INT),
        'flt': BasicMapping(type=FT.FLOAT),
        'random_1': BasicMapping(type=FT.OBJECT),
        'random_2': BasicMapping(type=FT.OBJECT)
    }
    i = EsIndex(root, idx, mapping)
    """
    i.delete_index()
    i.create_index()
    doc1 = {
        'text': '???????????????',
        'vector': [0.1, 0.4, 0.3, 0.2],
        'ts': {'a': 0.4, 'b': 0.2, 'c': 0.1},
        'f_ts': {'d': 0.4, 'e': 0.2, 'f': 0.1},
        'kw': 'city',
        'time': '2015-01-01',
        'int': 100,
        'flt': 12.4,
        'random_1': 'beijing',
        'random_2': [1, 2, '3']
    }
    doc2 = {
        'text': '???????????????',
        'vector': [1.0, -0.4, 3.3, 0.2],
        'ts': {'a': 0.1, 'b': 0.2, 'c': 0.4},
        'f_ts': {'d': 0.1, 'e': 0.2, 'f': 0.4},
        'kw': 'captial',
        'time': '2018-01-01',
        'int': 10,
        'flt': 2.4,
        'random_1': {123: 123},
    }
    doc3 = {
        'text': '???????????????',
        'vector': [0.5, 0.4, -2.3, -1.2],
        'ts': {'a': 0.1, 'b': 1.0, 'c': 0.1},
        'f_ts': {'d': 0.1, 'e': 1.0, 'f': 0.1},
        'kw': 'city',
        'time': '2016-01-01',
        'int': 1000,
        'flt': 1200.4,
        'random_2': {'ok': 23},
    }

    i.add(doc1, 1)
    i.add([doc2, doc3], [2, 3])

    time.sleep(2)

    x = i.rank({'text': '??????',
                'vector': [1.0, -0.4, 3.3, 0.2],
                'ts': ['a', 'b'],
                'f_ts': ['f']
                },
           "int>3 AND flt < 3000 AND time>2010-01-01", 10)
    print(x)

    x = i.rank({},
           "int>3 AND flt < 3000 AND time>2010-01-01", 10)
    print(x)
    """
    last_id = None
    for _ in range(4):
        x, last_id = i.scroll({}, None, 2, sort=None, search_after=last_id)
        print(x, last_id)
