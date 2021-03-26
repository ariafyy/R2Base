from r2base.engine.indexer import Indexer
from r2base.engine.ranker import Ranker
import numpy as np


if __name__ == "__main__":
    indexer = Indexer()
    ranker = Ranker()
    num_dim = 100
    mapping = {
        'doc_id': {'type': 'keyword'},
        'v': {'type': 'vector', 'num_dim': num_dim}
    }
    index = 'v-test'
    docs = []
    n = 100
    for i in range(n):
        docs.append({'doc_id': str(i), 'v': np.random.random(num_dim).tolist()})

    indexer.delete_index(index)
    indexer.create_index(index, mapping)
    indexer.add_docs(index, docs)
    import time
    time.sleep(2)
    query = {
        'match': {},
        'reduce': {
            'small_v': {
                'method': "umap",
                'input': "v",
                "kwargs": {
                    "n_components": 2
                }
            }
        }
    }
    print(ranker.query(index, query))

    query = {
        'match': {},
        'reduce': {
            'small_v': [
                {
                    'method': "pca",
                    'input': "v",
                    "kwargs": {
                        "n_components": 20
                    }
                },
                {
                    'method': "umap",
                    "kwargs": {
                        "n_components": 2
                    }
                }
            ]
        }
    }
    print(ranker.query(index, query))




