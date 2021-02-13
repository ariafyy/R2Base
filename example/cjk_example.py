from r2base.engine.indexer import Indexer
from r2base.engine.ranker import Ranker
import json
import numpy as np


def wiki2doc(l):
    temp = json.loads(l)
    docs = [{'title': temp[0],
             'text': temp[1][0:1000],
             'seed': np.random.randint(0, 1000)}]
    return docs


if __name__ == "__main__":
    path = "data/tiny_zh_wiki.jsonl"
    indexer = Indexer()
    ranker = Ranker()

    mapping = {
        'title': {'type': 'keyword'},
        'text': {'type': 'text',
                 'lang': 'zh',
                 'index': 'bm25'
                 }
    }
    docs = []
    chunk_size = 10
    index = 'wiki_zh'
    indexer.delete_index(index)
    indexer.create_index(index, mapping)
    cnt = 0
    with open(path, 'r', encoding='utf8') as f:
        buffer = []
        for l in f:
            cnt += 1
            buffer.extend(wiki2doc(l))
            if len(buffer) % chunk_size == 0:
                indexer.add_docs(index, buffer, chunk_size, show_progress=True)
                buffer = []

    print("DONE")
    print(ranker.query(index, {'match': {'text': "数学"}}))
