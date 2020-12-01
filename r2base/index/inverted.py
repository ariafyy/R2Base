from r2base.index import IndexBase
from r2base import IndexType as IT
from collections import defaultdict
from typing import List, Tuple, Union, Dict
import numpy as np
import os
import tantivy


class InvertedIndex(IndexBase):
    type = IT.CUS_INVERTED

    def create_index(self):
        pass

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


class BM25Index(IndexBase):
    type = IT.BM25

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        self._writer = None
        self._searcher = None
        self._client = None

    @property
    def client(self):
        if self._client is None:
            self._client = tantivy.Index.open(self.work_dir)
        return self._client

    @property
    def writer(self):
        if self._writer is None:
            self._writer = self.client.writer()
        return self._writer

    @property
    def searcher(self):
        if self._searcher is None:
            self._searcher = self.client.searcher()

        return self._searcher

    def create_index(self) -> None:
        if not os.path.exists(self.work_dir):
            os.mkdir(self.work_dir)
            self.logger.info("Create data folder at {}".format(self.work_dir))

        schema_builder = tantivy.SchemaBuilder()
        schema_builder.add_text_field("text", stored=False)
        schema_builder.add_integer_field("_id", stored=True, indexed=True)
        schema = schema_builder.build()
        tantivy.Index(schema, self.work_dir)

    def add(self, data: Union[List[str], str], doc_ids: Union[List[int], int]) -> None:
        if type(data) is str:
            data = [data]
            doc_ids = [doc_ids]

        for text, doc_id in zip(data, doc_ids):
            self.writer.add_document(tantivy.Document(text=text, _id=doc_id))
        self.writer.commit()
        self.client.reload()

    def delete(self, doc_ids: Union[List[int], int]) -> None:
        if type(doc_ids) is int:
            doc_ids = [doc_ids]

        for doc_id in doc_ids:
            self.writer.delete_documents(field_name='_id', field_value=doc_id)

        self.writer.commit()
        self.client.reload()

    def size(self) -> int:
        self._searcher = None
        return self.searcher.num_docs

    def rank(self, text: str, top_k: int) -> List[Tuple[float, int]]:
        """
        :param tokens: tokenized query
        :return:
        """
        query = self.client.parse_query(text, ["text"])
        res = self.searcher.search(query, top_k)
        results = [(h[0], self.searcher.doc(h[1])['_id'][0]) for h in res.hits]
        return results


if __name__ == "__main__":
    root = '.'
    idx = 'test-3'
    i = BM25Index(root, idx, {})
    i.create_index()
    i.add('我 来 自 上海，叫做 赵天成', 1)
    i.add('我 来 自 北京，叫做 赵天成', 2)
    i.add('我 来 自 北京，叫做 赵天成', 3)
    print(i.size())
    i.delete(1)
    print(i.rank('我', 2))
    print(i.size())
