from r2base.index.index import Index
from r2base.mappings import BasicMapping, VectorMapping, TextMapping
import pytest
import time

WORK_DIR = "."


def test_scroll_query():
    mapping = {'f1': TextMapping(type='text', lang="en"),
               'f5': TextMapping(type='text', lang="zh"),
               'f2': BasicMapping(type='integer'),
               'f3': BasicMapping(type='float'),
               "f4": BasicMapping(type='datetime')}

    i = Index(WORK_DIR, 'test_scroll_index')
    i.delete_index()
    i.create_index(mapping)
    time.sleep(1)

    docs = [{'f1': "New York", "f2": 10, "f4": '2019-06-28', 'f5': '北京'},
            {'f1': "New Land", "f2": 5, "f3": 10.3, 'f5': '北方'},
            {'f1': "New Town", "f2": 12, "f3": 3.3, 'f5': '北京故宫'},
            {'f1': "Old Land", "f2": 0, "f3": 5.3, '_uid': 'ddeeff', 'f5': '南宁'},
            {'f1': "Old Boy", "f2": 22, "f3": 1.1, '_uid': 'aabbcc', 'f5': '南方'}
            ]
    i.add_docs(docs, batch_size=2)
    time.sleep(2)

    def _scroll(match, filter, batch_size, adv_match=None):
        res = []
        temp, last_id = i.scroll_query({'match': match, 'filter': filter,
                                        'size': batch_size, 'adv_match': adv_match,})
        res.extend(temp)
        while last_id is not None:
            temp, last_id = i.scroll_query({'match': match,
                                            'filter': filter,
                                            'size': batch_size,
                                            'adv_match': adv_match,
                                            'search_after': last_id})
            res.extend(temp)
        return res

    scroll_docs = _scroll(None, None, 2)
    assert len(scroll_docs) == 5

    scroll_docs = _scroll(None, 'f2>10', 2)
    assert len(scroll_docs) == 2

    scroll_docs = _scroll({"f1": "New"}, "f2>5", 2)
    assert len(scroll_docs) == 2

    scroll_docs = _scroll({"f1": "New", "f5": "我在北京天坛"}, None, 2)
    assert len(scroll_docs) == 3

    scroll_docs = _scroll({"f1": "New", "f5": "我在北京天坛"}, None, 2)
    assert len(scroll_docs) == 3

    scroll_docs = _scroll(None, None, 2, adv_match={'bool': {'must': [{'match': {'f1': "New"}},
                                                                      {'match': {'f5': "北京"}}]}})
    assert len(scroll_docs) == 2

    i.delete_index()


def test_delete_query():
    mapping = {'f1': TextMapping(type='text', lang="en"),
               'f5': TextMapping(type='text', lang="zh"),
               'f2': BasicMapping(type='integer'),
               'f3': BasicMapping(type='float'),
               "f4": BasicMapping(type='datetime')}

    i = Index(WORK_DIR, 'test_query_delete')
    i.delete_index()
    i.create_index(mapping)
    time.sleep(1)

    docs = [{'f1': "New York", "f2": 10, "f4": '2019-06-28', 'f5': '北京'},
            {'f1': "New Land", "f2": 5, "f3": 10.3, 'f5': '北方'},
            {'f1': "New Town", "f2": 12, "f3": 3.3, 'f5': '北京故宫'},
            {'f1': "Old Land", "f2": 0, "f3": 5.3,  'f5': '南宁'},
            {'f1': "Old Boy",  "f2": 22, "f3": 1.1,  'f5': '南方'}
            ]
    i.add_docs(docs, batch_size=2)
    time.sleep(2)

    assert i.size() == 5
    res = i.delete_query({'filter': 'f2>20'})
    assert res['deleted'] == 1
    time.sleep(2)
    assert i.size() == 4

    res = i.delete_query({'filter': '*'})
    assert res['deleted'] == 4
    time.sleep(2)
    assert i.size() == 0

