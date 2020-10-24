from r2base.engine.indexer import Indexer
from r2base.engine.ranker import Ranker
import numpy as np

if __name__ == "__main__":
    indexer = Indexer()
    ranker = Ranker()

    mapping = {
        'key': {'type': 'keyword'},
        'text1': {'type': 'text',
                  'lang': 'en',
                  'index': 'inverted',
                  'model': 'bm25'},
        'vector': {'type': 'vector',
                   'num_dim': '4'},
        'text2': {'type': 'text',
                  'lang': 'en',
                  'index': 'vector',
                  'model': 'bert-base-uncased'}
    }
    doc1 = {
        'key': '123',
        'text1': 'hello i am from New York',
        'vector': np.array([0, 1, 2, 3]),
        'text2': 'how are you doing?'
    }
    doc2 = {
        'key': '234',
        'text1': 'hello i am from New York',
        'vector': np.array([0, 1, 3, 1]),
        'text2': 'how are you doing?'
    }

    index = 'test'
    indexer.create_index(index, mapping)
    doc_id = indexer.add_doc(index, [doc1, doc2])
    print(ranker.read_doc(index, doc_id))
    print(ranker.query(index, {'query': {'key': '234', 'vector': np.array([1, 1, 2, 3])}}))

    index = 'test2'
    indexer.create_index(index, mapping)
    doc_id = indexer.add_doc(index, [doc1, doc2])
    print(ranker.read_doc(index, doc_id))
    print(ranker.query(index, {'query': {'key': '123'}}))