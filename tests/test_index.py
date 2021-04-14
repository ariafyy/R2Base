from r2base.index.index import Index
from r2base.mappings import BasicMapping
import pytest
import time

WORK_DIR = "."


def test_basic_crud():
    mappings =  {'f1': {'type': 'keyword'},
                'f2': {'type': 'integer'},
                'f3': {'type': 'float'},
                'f4': {'type': 'datetime'}}

    i = Index(WORK_DIR, 'test_crud_index')
    i.delete_index()
    i.create_index(mappings)

    # get mapping
    dump_mapping = i.get_mappings()
    for k in ['f1', 'f2', 'f3', 'f4', '_uid']:
        assert k in dump_mapping

    assert i.size() == 0

    # add, read, delete, update
    docs = [{'f1': "haha", "f2": 10, "f4": '2019-06-28'},
            {'f1': "lala", "f3": 10.3},
            {'f2': 12, "f3": 3.3},
            {'f2': 0, "f3": 5.3, '_uid': 'ddeeff'},
            {'f2': 22, "f3": 1.1, '_uid': 'aabbcc'}
            ]
    # add docs
    doc_ids = i.add_docs(docs, batch_size=2)
    time.sleep(2)
    assert doc_ids[3] == 'ddeeff'
    assert doc_ids[4] == 'aabbcc'
    assert i.size() == 5

    # read docs
    docs = i.read_docs(['ddeeff', 'aabbcc'])
    assert len(docs) == 2
    docs = i.read_docs('ddeeff')
    assert len(docs) == 1
    assert docs[0]['_uid'] == 'ddeeff'

    # update docs
    doc_ids = i.update_docs({'f2': 44, "f3": 1.1, '_uid': 'aabbcc'}, batch_size=2)
    time.sleep(1)
    assert len(doc_ids) == 1
    assert doc_ids[0] == 'aabbcc'
    docs = i.read_docs('aabbcc')
    assert docs[0]['f2'] == 44

    # delete docs
    res = i.delete_docs('aabbcc')
    time.sleep(1)
    docs = i.read_docs('aabbcc')
    assert len(docs) == 0

    i.delete_index()



