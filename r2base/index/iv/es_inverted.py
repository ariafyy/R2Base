from r2base import IndexType as IT
from r2base.index.util_bases import EsBaseIndex
from r2base.config import EnvVar
from r2base.mappings import TextMapping
from typing import List, Tuple, Union, Dict
import numpy as np


class EsBM25Index(EsBaseIndex):
    type = IT.BM25

    def create_index(self) -> None:
        mapping: TextMapping = self.mapping
        params = {"timeout": '100s'}
        setting = EnvVar.deepcopy(EnvVar.ES_SETTING)

        if 'tokenize' in mapping.processor:
            self.logger.info("Detected customized tokenizer. Using cutter_analyzer")
            config = {
                'mappings': {'properties': {'text': {'type': 'text', "analyzer": "cutter_analyzer"}}},
                'settings': setting
            }
        else:
            config = {
                'mappings': {'properties': {'text': {'type': 'text'}}},
                'settings': setting
            }

        self._make_index(self.index_id, config, params)

    def add(self, data: Union[List[str], str], doc_ids: Union[List[int], int]) -> None:
        if type(data) is str:
            data = [data]
            doc_ids = [doc_ids]

        index_data = []
        for text, doc_id in zip(data, doc_ids):
            index_data.append({
                '_id': doc_id,
                'text': text,
                '_op_type': 'index',
                '_index': self.index_id
            })

        self.run_bulk(index_data, 5000)

    def rank(self, text: str, top_k: int) -> List[Tuple[float, int]]:
        """
        :param tokens: tokenized query
        :return:
        """
        query = {
            "_source": False,
            "query": {"match": {"text": text}},
            "size": top_k
        }
        res = self.es.search(index=self.index_id, body=query)
        results = [(float(h['_score']), int(h['_id'])) for h in res['hits']['hits']]
        return results


class EsInvertedIndex(EsBaseIndex):
    type = IT.INVERTED

    def create_index(self) -> None:
        params = {"timeout": '100s'}
        setting = EnvVar.deepcopy(EnvVar.ES_SETTING)

        config = {
            'mappings': {
                'properties': {"term_scores": {"type": "rank_features"}}
            },
            'settings': setting
        }
        self._make_index(self.index_id, config, params)

    def add(self, data: Union[List[Dict[str, float]], Dict[str, float]], doc_ids: Union[List[int], int]) -> None:
        if type(doc_ids) is int:
            data = [data]
            doc_ids = [doc_ids]

        index_data = []
        for ts, doc_id in zip(data, doc_ids):
            ts = {term: np.exp(s) - 1.0 for term, s in ts.items()}
            index_data.append({
                '_id': doc_id,
                'term_scores': ts,
                '_op_type': 'index',
                '_index': self.index_id
            })

        self.run_bulk(index_data, 5000)

    def rank(self, tokens: List[str], top_k: int) -> List[Tuple[float, int]]:
        """
        :param tokens: tokenized query
        :return:
        """
        main_query = [{'rank_feature': {'field': 'term_scores.{}'.format(t),
                                        "log": {"scaling_factor": 1.0}
                                        }} for t in tokens]
        es_query = {
            "_source": False,
            "query": {"bool": {"should": main_query}},
            "size": top_k
        }
        res = self.es.search(index=self.index_id, body=es_query)
        results = [(float(h['_score']), int(h['_id'])) for h in res['hits']['hits']]
        return results


class EsQuantInvertedIndex(EsBaseIndex):
    type = IT.INVERTED
    ALPHA = 10.0
    MAX_NUM_COPY = 100

    def create_index(self) -> None:
        params = {"timeout": '100s'}
        setting = EnvVar.deepcopy(EnvVar.ES_SETTING)

        config = {
            'mappings': {
                'properties': {"term_scores": {"type": "text", "analyzer": "cutter_analyzer", 'similarity': "tf_alone"}}
            },
            'settings': setting
        }
        self._make_index(self.index_id, config, params)

    def add(self, data: Union[List[Dict[str, float]], Dict[str, float]], doc_ids: Union[List[int], int]) -> None:
        if type(doc_ids) is int:
            data = [data]
            doc_ids = [doc_ids]

        index_data = []
        for ts, doc_id in zip(data, doc_ids):
            tokens = []
            for term, s in ts.items():
                tokens.extend([term] * min(self.MAX_NUM_COPY, int(s * self.ALPHA)))

            index_data.append({
                '_id': doc_id,
                'term_scores': '/'.join(tokens),
                '_op_type': 'index',
                '_index': self.index_id
            })

        self.run_bulk(index_data, 5000)

    def rank(self, tokens: List[str], top_k: int) -> List[Tuple[float, int]]:
        """
        :param tokens: tokenized query
        :return:
        """
        es_query = {
            "_source": False,
            "query": {"match": {"term_scores": '/'.join(tokens)}},
            "size": top_k
        }
        res = self.es.search(index=self.index_id, body=es_query)
        results = [(float(h['_score'])/self.ALPHA, int(h['_id'])) for h in res['hits']['hits']]
        return results


if __name__ == "__main__":
    import time
    root = '.'
    idx = 'test-3'
    i = EsQuantInvertedIndex(root, idx, {})
    i.delete_index()
    i.create_index()
    i.add({'a': 1.1, 'b': 2.2}, 1)
    i.add({'a': 1.1, 'c': 3.2}, 2)
    i.add({'c': 0.1, 'b': 2.2}, 3)

    time.sleep(2)
    x = i.rank(['a', 'c'], 2)
    print(x)

    exit(1)
    i = EsBM25Index(root, idx, {})
    i.delete_index()
    i.create_index()
    i.add('我 来 自 上海，叫做 赵天成', 1)
    i.add('我 来 自 北京，叫做 赵天成', 2)
    i.add('我 来 自 北京，叫做 赵天成', 3)
    time.sleep(2)
    print(i.size())
    i.delete(1)
    print(i.rank('我', 2))

    i.add('I am from New York', 4)
    i.add('I am from Los Angeles', 5)
    i.add('I am from Beijing', 6)
    time.sleep(2)
    print(i.size())
    i.delete(4)
    print(i.size())
    print(i.rank('I', 2))
    print(i.size())
