from r2base.index import IndexBase
from r2base import IndexType as IT
from typing import List, Tuple, Union, Dict
import os
import shutil
import re
import tantivy


class TyBM25Index(IndexBase):
    type = IT.BM25

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        self._writer = None
        self._searcher = None
        self._client = None

    def _normalize(self, text):
        return re.sub('["\[\]{\}:/)(-]', '', text).replace("\\","")

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

    def delete_index(self) -> None:
        try:
            shutil.rmtree(self.work_dir)
        except Exception as e:
            self.logger.error(e)

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
        text = self._normalize(text).strip()
        if not text:
            return []
        try:
            query = self.client.parse_query(text, ["text"])
        except Exception as e:
            self.logger.error("Error on searching {}".format(text))
            raise e
        res = self.searcher.search(query, top_k)
        results = [(h[0], self.searcher.doc(h[1])['_id'][0]) for h in res.hits]
        return results


if __name__ == "__main__":
    root = '.'
    idx = 'test-3'
    i = TyBM25Index(root, idx, {})
    i.delete_index()
    i.create_index()
    i.add('我 来 自 上海，叫做 赵天成', 1)
    i.add('我 来 自 北京，叫做 赵天成', 2)
    i.add('我 来 自 北京，叫做 赵天成', 3)
    print(i.size())
    i.delete(1)
    print(i.rank('我', 2))
    print(i.size())
