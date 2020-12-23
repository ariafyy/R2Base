import requests
import os

host_url = "http://localhost:8000"

def delete_index(index_id):
    requests.delete(url=os.path.join(host_url, 'r2base/v1/index/{}'.format(index_id)))


def make_index(index_id, mapping):
    requests.delete(url=os.path.join(host_url, 'r2base/v1/index/{}'.format(index_id)))
    res = requests.post(url=os.path.join(host_url, 'r2base/v1/index/{}'.format(index_id)),
                        json={'mappings': mapping})
    return res.json()


def add_docs(index_id, docs):
    res = requests.post(url=os.path.join(host_url, 'r2base/v1/index/{}/docs'.format(index_id)),
                        json={'docs': docs, 'batch_siz': 100})
    return res.json()


def search(index_id, query):
    res = requests.post(url=os.path.join(host_url, 'r2base/v1/search/{}/query'.format(index_id)),
                        json={'query': query})
    return res.json()

if __name__ == "__main__":
    mapping = {
        'doc_id': {'type': 'keyword'},
        'tss': {'type': 'term_score'}
    }
    index = 'ts-testt'
    docs = []
    docs.append({'doc_id': '1', 'tss': {'a': 1.1, 'b': 1.2, 'c': 1.3}})
    docs.append({'doc_id': '2', 'tss': {'b': 1.1, 'c': 1.2, 'd': 1.3}})
    docs.append({'doc_id': '3', 'tss': {'c': 1.1, 'd': 1.2, 'f': 1.3}})

    delete_index(index)
    make_index(index, mapping)
    add_docs(index, docs)
    import time
    time.sleep(2)
    print(search(index, {'match': {'tss': ['a', 'b']}}))



