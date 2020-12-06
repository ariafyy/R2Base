import numpy as np
from r2base.index import IndexBase
from r2base import IndexType as IT
import faiss
from typing import Dict, Union, List, Tuple
import os


class VectorIndex(IndexBase):
    type = IT.VECTOR

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        super().__init__(root_dir, index_id, mapping)
        self._num_dim = mapping['num_dim']
        self._index = None
        self.ivf_th = 10000

    def _commit(self):
        faiss.write_index(self._index, os.path.join(self.work_dir, 'data.index'))

    @property
    def index(self) -> faiss.Index:
        if self._index is None:
            self._index = faiss.read_index(os.path.join(self.work_dir, 'data.index'))
        return self._index

    def create_index(self):
        if not os.path.exists(self.work_dir):
            os.mkdir(self.work_dir)
            self.logger.info("Create data folder at {}".format(self.work_dir))
        if not os.path.exists(os.path.join(self.work_dir, 'data.index')):
            index = faiss.index_factory(self._num_dim, 'IDMap,L2norm,Flat', faiss.METRIC_INNER_PRODUCT)
            faiss.write_index(index, os.path.join(self.work_dir, 'data.index'))
        else:
            raise Exception("Index {} already existed".format(self.index_id))

    def size(self) -> int:
        return self.index.ntotal

    def add(self, vector: np.array, doc_ids: Union[List[int], int]):
        if len(vector.shape) == 1:
            vector = vector.reshape(1, -1)
        if type(doc_ids) is int:
            doc_ids = [doc_ids]

        assert vector.shape[1] == self._num_dim
        assert vector.shape[0] == len(doc_ids)
        # normalize the vector
        vector = vector.astype('float32')
        self.index.add_with_ids(vector, np.array(doc_ids))
        self._commit()

    def delete(self, doc_ids: Union[List[int], int]) -> None:
        if type(doc_ids) is int:
            doc_ids = [doc_ids]

        self.index.remove_ids(np.array(doc_ids))
        self._commit()

    def rank(self, vector: np.array, top_k: int) -> List[Tuple[float, int]]:
        """
        :param vector:
        :param top_k:
        :return: [(score, _id), ....]
        """
        if len(vector.shape) == 1:
            vector = vector.reshape(1, -1)

        assert vector.shape[0] == 1
        vector = vector.astype('float32')
        dists, indices = self.index.search(vector, top_k)
        res = [(dists[0, i], indices[0, i]) for i in range(dists.shape[1])]
        return res


if __name__ == '__main__':
    import uuid
    index = VectorIndex('.', 'test', {'num_dim': 800})
    #index.create_index()

    #for i in range(100):
    #    print(i)
    #    ids = [i*100+j for j in range(100)]
    #    index.add(np.random.random(800*100).reshape(100, 800),ids)
    print(index.size())
    index.delete([1,2,33])
    index.delete([5061])
    print(index.size())
    print(index.rank(np.random.random(800*1).reshape(1, 800), 10))
