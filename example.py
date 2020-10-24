from r2base.engine.pipeline import Pipeline
import numpy as np

if __name__ == "__main__":
    pipeline = Pipeline()

    mapping = {
        'key': {'type': 'keyword'},
        'text1': {'type': 'text',
                  'lang': 'en',
                  'index': 'inverted',
                  'model': 'bm25'},
        'vector': {'type': 'vector',
                   'dim': '4'},
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
        'vector': np.array([0, 1, 2, 3]),
        'text2': 'how are you doing?'
    }

    index = 'test'
    pipeline.create_index(index, mapping)
    doc_id = pipeline.add_doc(index, [doc1, doc2])
    print(pipeline.read_doc(index, doc_id))
    print(pipeline.rank(index, {'query': {'key': '234'}}))

    index = 'test2'
    pipeline.create_index(index, mapping)
    doc_id = pipeline.add_doc(index, [doc1, doc2])
    print(pipeline.read_doc(index, doc_id))
    print(pipeline.rank(index, {'query': {'key': '123'}}))