from r2base.engine.indexer import Indexer
from r2base.engine.ranker import Ranker
import json
from nltk import sent_tokenize
import numpy as np


def wiki2sents(l):
    temp = json.loads(l)
    sents = sent_tokenize(temp[1])[0:50]
    docs = []
    if len(sents) == 1:
        text = sents[0]
        docs.append({'title': temp[0], 'text': text})
    else:
        for s_id, s in enumerate(sents):
            if len(s) < 10:
                continue

            if s_id == 0:
                text = '<a>' + s + '</a>' + sents[s_id + 1]
            elif s_id == len(sents) - 1:
                text = sents[s_id - 1] + '<a>' + s + '</a>'
            else:
                text = sents[s_id - 1] + '<a>' + s + '</a>' + sents[s_id + 1]

            docs.append({'title': temp[0], 'text': text})
    return docs


def wiki2doc(l):
    temp = json.loads(l)
    docs = [{'title': temp[0], 'text': temp[1][0:1000], 'seed': np.random.randint(0, 1000)}]
    return docs


if __name__ == "__main__":
    path = "example/tiny_wiki.jsonl"
    indexer = Indexer()
    ranker = Ranker()

    mapping = {
        'title': {'type': 'keyword'},
        'text': {'type': 'text',
                 'lang': 'en',
                 'index': 'bm25',
                 'processor': 'nothing'
                 }
    }
    docs = []
    chunk_size = 100
    index = 'wiki-es'
    indexer.delete_index(index)
    indexer.create_index(index, mapping)
    cnt = 0
    with open(path, 'rb') as f:
        buffer = []
        for l in f:
            cnt += 1
            buffer.extend(wiki2doc(l))
            if len(buffer) % chunk_size == 0:
                indexer.add_docs(index, buffer, chunk_size, show_progress=True)
                buffer = []

    print("DONE")
