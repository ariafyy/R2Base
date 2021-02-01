import requests
import os
import json
import numpy as np


host_url = "http://localhost:8000"


def delete_index(index_id):
    res = requests.delete(url=os.path.join(host_url, 'r2base/v1/index/{}'.format(index_id)))
    if res.status_code > 300:
        raise Exception(res.json())


def make_index(index_id, mapping):
    res = requests.post(url=os.path.join(host_url, 'r2base/v1/index/{}'.format(index_id)),
                        json={'mappings': mapping})
    if res.status_code > 300:
        raise Exception(res.json())
    return res.json()


def add_docs(index_id, docs):
    res = requests.post(url=os.path.join(host_url, 'r2base/v1/index/{}/docs'.format(index_id)),
                        json={'docs': docs, 'batch_siz': 100})
    if res.status_code > 300:
        raise Exception(res.json())
    return res.json()


def search(index_id, query):
    res = requests.post(url=os.path.join(host_url, 'r2base/v1/search/{}/query'.format(index_id)),
                        json={'query': query})
    if res.status_code > 300:
        raise Exception(res.json())
    return res.json()


def wiki2doc(l):
    temp = json.loads(l)
    docs = [{'title': temp[0], 'text': temp[1][0:1000], 'seed': np.random.randint(0, 1000)}]
    return docs


if __name__ == "__main__":
    path = "data/tiny_wiki.jsonl"

    mapping = {
        'title': {'type': 'keyword'},
        'text': {'type': 'text',
                 'lang': 'en',
                 'index': 'bm25'
                 }
    }
    docs = []
    chunk_size = 10
    index = 'wiki-es'
    delete_index(index)
    make_index(index, mapping)
    cnt = 0
    with open(path, 'rb') as f:
        buffer = []
        for l in f:
            cnt += 1
            buffer.extend(wiki2doc(l))
            if len(buffer) % chunk_size == 0:
                add_docs(index, buffer)
                buffer = []

    print("DONE")
    print(search(index, {'match': {'text': "brazil"}}))

