import numpy as np


class VectorIndex(object):
    def __init__(self, index_id):
        self.index_id = index_id
        self._index = None
        self._ids = []

    def _norm(self, A):
        return A / np.linalg.norm(A, ord=2, axis=1, keepdims=True)

    def _cosine(self, A, B):
        A_norm_ext = self._norm(A)
        B_norm_ext = self._norm(B)
        return A_norm_ext.dot(B_norm_ext).clip(min=0) / 2

    def add(self, key, vector):
        if self._index is None:
            self._index = vector.reshape(-1, 1)
        else:
            self._index = np.concatenate([self._index, vector.reshape(-1, 1)], axis=0)

        self._ids.append(key)
        return self._index.shape

    def rank(self, vector, top_k):
        """
        :param vector:
        :param top_k:
        :return: [(score, _id), ....]
        """
        vector = vector.reshape(-1, 1)
        scores = self._cosine(self._index, vector)
        sort_ids = np.argsort(scores*-1)
        res = []
        for idx in sort_ids:
            res.append((scores[idx], self._ids[idx]))

        return res[0:top_k]