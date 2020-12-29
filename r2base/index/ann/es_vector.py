import numpy as np
from numpy import ndarray
from r2base import IndexType as IT
from r2base.config import EnvVar
from r2base.index.util_bases import EsBaseIndex
from r2base.mappings import VectorMapping
from typing import Dict, Union, List, Tuple


class EsVectorIndex(EsBaseIndex):
    type = IT.VECTOR

    def __init__(self, root_dir: str, index_id: str, mapping: VectorMapping):
        super().__init__(root_dir, index_id, mapping)
        self._num_dim = mapping.num_dim

    def create_index(self):
        mapping: VectorMapping = self.mapping
        setting = EnvVar.ES_SETTING
        setting.pop('analysis')
        params = {"timeout": '100s'}
        config = {
            'mappings': {
                'properties': {'vector': {'type': 'dense_vector', 'dims': self._num_dim}}
            },
            'settings': setting
        }
        self._make_index(self.index_id, config, params)

    def add(self, vector: Union[ndarray, List], doc_ids: Union[List[int], int]):
        if type(vector) is list:
            vector = np.array(vector)
        vector = vector.astype('float32')

        if len(vector.shape) == 1:
            vector = vector.reshape(1, -1)

        if type(doc_ids) is int:
            doc_ids = [doc_ids]

        index_data = []
        for v, doc_id in zip(vector, doc_ids):
            if type(v) is ndarray:
                v = v.tolist()

            index_data.append({
                '_id': doc_id,
                'vector': v,
                '_op_type': 'index',
                '_index': self.index_id
            })

        self.run_bulk(index_data, 5000)

    def rank(self, vector: Union[ndarray, List], top_k: int) -> List[Tuple[float, int]]:
        """
        :param vector:
        :param top_k:
        :return: [(score, _id), ....]
        """
        if type(vector) is ndarray:
            vector = vector.tolist()
        vector = [float(v) for v in vector]
        assert len(vector) == self._num_dim
        query = {
            "_source": False,
            'query': {
                "script_score": {
                    'query': {"match_all": {}},
                    "script": {
                        "source": "cosineSimilarity(params.query_vector, 'vector') + 1.0",
                        "params": {
                            "query_vector": vector
                        }
                    }}},
            "size": top_k
        }
        res = self.es.search(index=self.index_id, body=query)
        results = [(float(h['_score']/2.0), int(h['_id'])) for h in res['hits']['hits']]
        return results

if __name__ == '__main__':
    index = EsVectorIndex('..', 'test', VectorMapping(type='vector', num_dim=4))

    index.delete_index()
    index.create_index()

    for i in range(10):
        print(i)
        ids = [i*100+j for j in range(100)]
        index.add(np.random.random(4*100).reshape(100, 4),ids)
    import time
    time.sleep(2)

    print(index.size())
    #index.delete([1,2,33])
    #index.delete([5061])
    print(index.size())
    print(index.rank(np.random.random(4*1), 10))
