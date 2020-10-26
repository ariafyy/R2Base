from r2base.engine.indexer import Indexer
from r2base.engine.ranker import Ranker
import numpy as np

if __name__ == "__main__":
    indexer = Indexer()
    ranker = Ranker()

    mapping = {
        'key':   {'type': 'keyword'},
        'text1': {'type': 'text',
                  'lang': 'en',
                  'index': 'bm25',
                  'processor': 'lower,tokenize'
                  },
        'vector': {'type': 'vector',
                   'num_dim': 4},
        'text2': {'type': 'text',
                  'lang': 'en',
                  'index': 'vector',
                  "num_dim": 768,
                  'processor': 'py_encode',
                  'model_id': 'bert-base-nli-stsb-mean-tokens'
                  },
        'text3': {'type': 'text',
                  'lang': 'en',
                  #'index': 'custom_inverted',
                  #'processor': 'encode',
                  #'q_processor': 'bert-bert-uncased',
                  #'model': 'bert-sparta'
                  }
    }
    doc1 = {
        'key': '123',
        'text1': 'hello i am from New York',
        'vector': np.array([0, 1, 2, 3]),
        'text2': 'how are you doing?',
        "text3": "I am doing fine."
    }
    doc2 = {
        'key': '234',
        'text1': 'hello i am from China',
        'vector': np.array([0, 1, 3, 1]),
        'text2': 'good bye',
        "text3": "I am doing bad."
    }

    index = 'test'
    indexer.create_index(index, mapping)
    doc_id = indexer.add_doc(index, [doc1, doc2])
    print(ranker.read_doc(index, doc_id))
    print(ranker.query(index, {'query': {'text2': 'bye bye',
                                         'text1': "i"}}))

    #'vector': np.array([1, 1, 2, 3])
