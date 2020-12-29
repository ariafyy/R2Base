import requests
import os
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


if __name__ == "__main__":
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

    delete_index(index)
    make_index(index, mapping)
    add_docs(index, docs)
    import time
    time.sleep(2)
    query = {
        'match': {},
        "exclude": ['v'],
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
    print(search(index, query))

    query = {
        'match': {},
        "exclude": ['v'],
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
    print(search(index, query))




