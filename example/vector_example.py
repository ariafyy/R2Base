import requests
import os


host_url = "http://localhost:5678"


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
    mapping = {
        'doc_id': {'type': 'keyword'},
        'v': {'type': 'vector', 'num_dim': 3},
        'v2': {'type': 'vector', 'num_dim': 3}

    }
    index = 'v-test'
    docs = []
    docs.append({'doc_id': '1', 'v': [1, 2, 3], 'v2': [1, 2, 3]})
    docs.append({'doc_id': '2', 'v': [-1, -2, -3], 'v2': [-1, -2, -3]})
    docs.append({'doc_id': '3', 'v': [7, 8, 9], 'v2': [7, 8, 9]})

    delete_index(index)
    make_index(index, mapping)
    add_docs(index, docs)
    import time
    time.sleep(2)
    print(search(index, {'match': {'v': [1, 2, 3]}}))
    print(search(index, {'match': {'v': {'value': [1,2,3], "threshold": 0.8},
                                   'v2': {'value': [-2,2,-3], "threshold": 0.0}}}))




