import numpy as np
from r2base.index import BaseIndex
from r2base import IndexType as IT


class VectorIndex(BaseIndex):
    type = IT.VECTOR

    def __init__(self, index_id, num_dim):
        self.index_id = index_id
        self.num_dim = num_dim
        self._index = None
        self._ids = []

    def _norm(self, A):
        return A / np.linalg.norm(A, ord=2, axis=1, keepdims=True)

    def _cosine(self, A, B):
        A_norm_ext = self._norm(A)
        B_norm_ext = self._norm(B)
        return A_norm_ext.dot(B_norm_ext.T).clip(min=0).squeeze(1) / 2

    def add(self, vector, key):
        if len(vector.shape) == 1:
            vector = vector.reshape(1, -1)

        assert vector.shape[1] == self.num_dim

        if self._index is None:
            self._index = vector
        else:
            self._index = np.concatenate([self._index, vector], axis=0)

        self._ids.append(key)
        return self._index.shape

    def rank(self, vector, top_k):
        """
        :param vector:
        :param top_k:
        :return: [(score, _id), ....]
        """
        if len(vector.shape) == 1:
            vector = vector.reshape(1, -1)
        scores = self._cosine(self._index, vector)
        sort_ids = np.argsort(scores*-1)
        res = []
        for idx in sort_ids:
            res.append((scores[idx], self._ids[idx]))

        return res[0:top_k]