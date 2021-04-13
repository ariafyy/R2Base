from r2base.engine.indexer import Indexer
from r2base.engine.ranker import Ranker


if __name__ == "__main__":
    mapping = {
        'tss':  {'type': 'term_score',
                 'q_processor': 'tf_tokenize',
                 'model_id': "bert-base-chinese-zh_v4-10K"
                 }
    }
    chunk_size = 100
    index = 'tss_tks'
    indexer = Indexer()
    ranker = Ranker()

    indexer.delete_index(index)
    indexer.create_index(index, mapping)
    docs =  [{'tss': {'我': 1.0, '是': 2.0}},
             {'tss': {'你': 2.0, '好': 1.0}}]

    indexer.add_docs(index, docs, 10, show_progress=True)
    print(ranker.query(index, {'match': {'tss': '你好'}}))



