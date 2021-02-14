from r2base import IndexType as IT
from r2base import FieldType as FT
from r2base.index.util_bases import EsBaseIndex
from r2base.config import EnvVar
from r2base.index.field_ops.text import TextField
from r2base.index.field_ops.iv import InvertedField
from r2base.index.field_ops.vector import VectorField
from r2base.index.field_ops.filter import FilterField
from typing import List, Union, Dict, Optional, Tuple


class EsIndex(EsBaseIndex):
    type = IT.RANK

    @property
    def valid_fields(self):
        fields = set()

        for field, mapping in self.mapping.items():
            if mapping.type in {FT.TEXT, FT.VECTOR, FT.TERM_SCORE}:
                fields.add(field)

            elif mapping.type in FT.FILTER_TYPES:
                fields.add(field)
        return fields

    def create_index(self) -> None:
        params = {"timeout": '60s'}
        setting = EnvVar.deepcopy(EnvVar.ES_SETTING)
        properties = {}

        for field, mapping in self.mapping.items():
            if mapping.type == FT.TEXT:
                properties[field] = TextField.to_mapping(mapping)

            elif mapping.type == FT.VECTOR:
                properties[field] = VectorField.to_mapping(mapping)

            elif mapping.type == FT.TERM_SCORE:
                properties[field] = InvertedField.to_mapping(mapping)

            elif mapping.type in FT.FILTER_TYPES:
                properties[field] = FilterField.to_mapping(mapping)

        config = {
            'mappings': {'properties': properties},
            'settings': setting
        }

        self._make_index(self.index_id, config, params)

    def add(self, data: Union[List[Dict], Dict], doc_ids) -> None:
        if type(data) is dict:
            data = [data]
            doc_ids = [doc_ids]

        index_data = []
        for doc, doc_id in zip(data, doc_ids):
            body = {}
            for field, value in doc.items():
                if field not in self.valid_fields:
                    continue

                mapping = self.mapping[field]
                if mapping.type == FT.TEXT:
                    body[field] = TextField.to_add_body(mapping, value)

                elif mapping.type == FT.VECTOR:
                    body[field] = VectorField.to_add_body(mapping, value)

                elif mapping.type == FT.TERM_SCORE:
                    body[field] = InvertedField.to_add_body(mapping, value)

                elif mapping.type in FT.FILTER_TYPES:
                    body[field] = FilterField.to_add_body(mapping, value)

            body['_id'] = doc_id
            body['_op_type'] = 'index'
            body['_index'] = self.index_id
            index_data.append(body)

        if len(index_data) > 0:
            self.run_bulk(index_data, 5000)
        else:
            self.logger.warn("Skip add since data is empty.")

    def _sql2json(self, sql_filter: str):
        sql_filter = 'SELECT * FROM "{}" WHERE {}'.format(self.index_id, sql_filter)
        res = self.es.sql.translate({'query': sql_filter})
        return res['query']

    def rank(self, match: Dict, sql_filter: Optional[str], top_k: int) -> List[Tuple[int, float]]:
        """
        :param match: a dictionary that contains match and filters
        :param ctx_filter: a SQL string None if not given
        :param top_k: return top_k
        :return:
        """
        # TODO: 1. scroll_query, 2. advanced query 3. threshold
        if sql_filter is not None and sql_filter:
            json_filter = self._sql2json(sql_filter)
        else:
            json_filter = None

        es_qs = []
        keys = []
        for field, value in match.items():
            if field not in self.valid_fields:
                continue
            mapping = self.mapping[field]
            body = None
            if mapping.type == FT.TEXT:
                body = TextField.to_query_body(field, mapping, value, top_k, json_filter)

            elif mapping.type == FT.VECTOR:
                body = VectorField.to_query_body(field, mapping, value, top_k, json_filter)

            elif mapping.type == FT.TERM_SCORE:
                body = InvertedField.to_query_body(field, mapping, value, top_k, json_filter)

            if body is not None:
                keys.append(field)
                es_qs.append({'index': self.index_id})
                es_qs.append(body)

        m_res = self.es.msearch(body=es_qs)
        m_ranks = {}
        for k_id, key in enumerate(keys):
            mapping = self.mapping[key]
            res = m_res['responses'][k_id]
            if mapping.type == FT.TEXT:
                m_ranks[key] = TextField.hits2ranks(mapping, res)

            elif mapping.type == FT.VECTOR:
                m_ranks[key] = VectorField.hits2ranks(mapping, res)

            elif mapping.type == FT.TERM_SCORE:
                m_ranks[key] = InvertedField.hits2ranks(mapping, res)

        # combine score from different fields
        fuse_res = dict()
        for key, ranks in m_ranks.items():
            for score, doc_id in ranks:
                fuse_res[doc_id] = score + fuse_res.get(doc_id, 0.0)

        ranks = [(k, v) for k, v in fuse_res.items()]
        ranks = sorted(ranks, key=lambda x: x[1], reverse=True)[0:top_k]
        return ranks


if __name__ == "__main__":
    import time
    from r2base.mappings import VectorMapping,TextMapping,TermScoreMapping, BasicMapping
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
        'flt': BasicMapping(type=FT.FLOAT)
    }
    i = EsIndex(root, idx, mapping)
    """
    i.delete_index()
    i.create_index()
    doc1 = {
        'text': '我是深圳人',
        'vector': [0.1, 0.4, 0.3, 0.2],
        'ts': {'a': 0.4, 'b': 0.2, 'c': 0.1},
        'f_ts': {'d': 0.4, 'e': 0.2, 'f': 0.1},
        'kw': 'city',
        'time': '2015-01-01',
        'int': 100,
        'flt': 12.4
    }
    doc2 = {
        'text': '我是北京人',
        'vector': [1.0, -0.4, 3.3, 0.2],
        'ts': {'a': 0.1, 'b': 0.2, 'c': 0.4},
        'f_ts': {'d': 0.1, 'e': 0.2, 'f': 0.4},
        'kw': 'captial',
        'time': '2018-01-01',
        'int': 10,
        'flt': 2.4
    }
    doc3 = {
        'text': '我是杭州人',
        'vector': [0.5, 0.4, -2.3, -1.2],
        'ts': {'a': 0.1, 'b': 1.0, 'c': 0.1},
        'f_ts': {'d': 0.1, 'e': 1.0, 'f': 0.1},
        'kw': 'city',
        'time': '2016-01-01',
        'int': 1000,
        'flt': 1200.4
    }

    i.add(doc1, 1)
    i.add([doc2, doc3], [2, 3])

    time.sleep(2)
    """
    x = i.rank({'text': '杭州',
                'vector': [1.0, -0.4, 3.3, 0.2],
                'ts': ['a', 'b'],
                'f_ts': ['f']
                },
           "int>3 AND flt < 3000 AND time>2010-01-01", 10)
    print(x)

