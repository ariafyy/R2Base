from r2base.index.es_full import EsIndex
from r2base.index.index import Index
from r2base.mappings import BasicMapping, VectorMapping, TextMapping
import pytest
import time

WORK_DIR = "."

"""
def test_filter_index():
    c = EsIndex(WORK_DIR, 'test_filter_index',
                    {'f1': BasicMapping(type='keyword'),
                     'f2': BasicMapping(type='integer'),
                     'f3': BasicMapping(type='float'),
                     "f4": BasicMapping(type='date'),
                     "f5": BasicMapping(type='datetime')})

    c.delete_index()
    c.create_index()
    c.add({'f1': "haha", "f2": 10, "f4": '2019-06-28'}, '123')
    c.add({'f1': "lala", "f3": 10.3, "f5": '2015-01-01T12:10:30Z'}, '456')
    c.add({'f2': 12, "f3": 3.3}, '666')
    c.add([{'f2': 0, "f3": 5.3}, {'f2': 22, "f3": 1.1}], ['789', '900'])
    time.sleep(1)
    assert len(c.rank(None, "f4=CAST('2019-06-28' as DATETIME)", 100)) == 1
    assert len(c.rank(None, "f5=CAST('2015-01-01T12:10:30' as DATETIME)", 100)) == 1

    assert len(c.rank(None, "f4 BETWEEN CAST('2019-05-28' AS DATETIME) AND CAST('2020-09-28' AS DATETIME)", 100)) == 1
    assert c.size() == 5
    assert len(c.rank(None, 'f2>1', 100)) == 3
    c.delete(['789', '666'])
    time.sleep(1)
    assert c.size() == 3
    assert len(c.rank(None, "f1='haha'", 100)) == 1
    assert len(c.rank(None, "f1='haha' AND _uid IN ('123', '456')", 100)) == 1
    assert len(c.rank(None, "f1='haha' AND _uid IN ('666', '456')", 100)) == 0
    c.delete_index()


def test_kv():
    c = KVIndex(WORK_DIR, 'test_kv', BasicMapping(type='_id'))
    c.delete_index()
    c.create_index()
    c.set(1, '123')
    c.set(2, '456')
    c.set(3, '789')
    c.set([4, 5, 6], ['789', '8910', '901'])
    assert c.size() == 6
    c.delete(3)
    assert c.size() == 5
    assert c.get(1) == '123'
    assert c.get([1,2]) == ['123', '456']
    assert len(c.sample(2)) == 2
    c.delete_index()


def test_bm25():
    i = EsBM25Index(WORK_DIR, 'test_bm25', TextMapping(type='text', index='bm25', lang='zh'))
    i.delete_index()
    i.create_index()
    i.add('我/来自/北京，叫做/赵天成', 1)
    i.add('我/来自/杭州，叫做/赵天成', 2)
    i.add(['我/来自/北京', '叫做/赵天成'], [3, 4])
    time.sleep(2)
    assert i.size() == 4
    assert len(i.rank('name', 10)) == 0
    assert len(i.rank('我', 10)) == 3
    i.delete([1, 2])
    time.sleep(2)
    assert i.size() == 2
    assert len(i.rank('北京', 10)) == 1
    i.delete_index()


def test_vector():
    index = EsVectorIndex(WORK_DIR, 'test_vector', VectorMapping(type='vector', num_dim=3))
    index.delete_index()
    index.create_index()
    index.add([1, 2, 3], 1)
    index.add([3, 2, 1], 2)
    index.add([[1, 2, 3], [4, 5, 6]], [3, 4])
    time.sleep(2)
    assert index.size() == 4
    assert index.rank([1, 2, 3], 10)[0][0] == pytest.approx(1.0)
    index.delete(1)
    time.sleep(2)
    assert index.size() == 3
    index.delete_index()


def test_inverted():
    i = EsInvertedIndex(WORK_DIR, 'test_es_iv', BasicMapping(type='inverted'))
    i.delete_index()
    i.create_index()
    i.add({'a': 1, 'b': 2}, 1)
    i.add([{'a': 1, 'c': 4}, {'d': 1, 'b': 2}], [2, 3])
    i.add({'z': 1, 'd': 2}, 4)
    import time
    time.sleep(2)
    assert i.size() == 4
    docs = i.rank(['b'], 10)
    assert len(docs) == 2
    assert docs[0][0] == pytest.approx(2.0, rel=0.1)
    i.delete(1)
    time.sleep(2)
    assert i.size() == 3
    docs = i.rank(['a', 'c'], 10)
    assert len(docs) == 1
    assert docs[0][0] == pytest.approx(5.0, rel=0.1)
    i.delete_index()


def test_index():
    i = Index(WORK_DIR, 'test_index')
    i.delete_index()
    i.create_index({})
    i.add_docs({"key": 123})
    i.add_docs({"key": 456})
    print(i.size())
    print(i.query({'query': {"match": {}}}))
    i.delete_index()
    i.create_index({})
    i.add_docs({"key": 123})
    i.add_docs({"key": 456})
    print(i.size())
    print(i.query({'query': {"match": {}}}))
"""