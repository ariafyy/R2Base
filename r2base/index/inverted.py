from r2base.index import BaseIndex
from r2base import IndexType as IT
from collections import defaultdict, Counter
from typing import List, Tuple, Union
import numpy as np


class InvertedIndex(BaseIndex):
    type = IT.CUS_INVERTED

    def __init__(self, index_id: str):
        self.index_id = index_id
        self._inverted_index = defaultdict(dict)

    def add(self, scores: List[Tuple], doc_id: str):
        for t, s in scores:
            self._inverted_index[t][doc_id] = np.float16(s)
        return True

    def rank(self, tokens: Union[List[str], List[Tuple]], top_k: int):
        temp = defaultdict(float)
        for t in tokens:
            if type(t) is tuple:
                weight, token = t
            else:
                weight, token = 1.0, t

            for doc_id, v in self._inverted_index.get(token, {}).items():
                temp[doc_id] += v * weight

        # merge by doc_ids
        results = sorted([(v, k) for k, v in temp.items()], reverse=True, key=lambda x: x[0])
        return results[0:top_k]


class BM25Index(BaseIndex):
    type = IT.BM25

    def __init__(self, index_id: str):
        self.index_id = index_id
        self._inverted_index = defaultdict(dict)
        self._doc_ids = set()

    def add(self, tokens: List[str], doc_id: str):
        scores = Counter(tokens).most_common()
        self._doc_ids.add(doc_id)
        for t, s in scores:
            self._inverted_index[t][doc_id] = np.int8(s)
        return True

    def rank(self, tokens: List[str], top_k: int):
        """
        :param tokens: tokenized query
        :return:
        """
        temp = defaultdict(float)
        for t in tokens:
            idf = np.log(len(self._doc_ids) / (len(self._inverted_index.get(t, {})) + 1e-5))

            for doc_id, v in self._inverted_index.get(t, {}).items():
                temp[doc_id] += float(v) * idf

        # merge by doc_ids
        results = sorted([(v, k) for k, v in temp.items()], reverse=True, key=lambda x: x[0])
        return results[0:top_k]