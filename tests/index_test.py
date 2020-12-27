from r2base.index.filter import FilterIndex
from r2base.index.keyvalue import KVIndex
from r2base.index.iv.ty_inverted import TyBM25Index
from r2base.index.iv.es_inverted import EsInvertedIndex
from r2base.index.ann.faiss_vector import FaissVectorIndex
from r2base.index.index import Index
import pytest

WORK_DIR = "."


def test_filter_index():
    c = FilterIndex(WORK_DIR, 'test_filter_index',
                    {'f1': {'type': "keyword"},
                     'f2': {"type": "integer"},
                     'f3': {"type": "float"},
                     "f4": {'type': "date"},
                     "f5": {'type': "datetime"}})

    c.delete_index()
    c.create_index()
    c.add({'f1': "haha", "f2": 10, "f4": '2019-06-28'}, 123)
    c.add({'f1': "lala", "f3": 10.3, "f5": '2020-06-28 20:57:32'}, 456)
    c.add({'f2': 12, "f3": 3.3}, 666)
    c.add([{'f2': 0, "f3": 5.3}, {'f2': 22, "f3": 1.1}],
          [789, 900])

    assert len(c.select('f4=date("2019-06-28")')) == 1
    assert len(c.select('f5=datetime("2020-06-28 20:57:32")')) == 1
    assert len(c.select('f4 BETWEEN date("2019-05-28") AND date("2019-07-28")')) == 1
    assert c.size() == 5
    assert len(c.select('f2>1')) == 3
    c.delete([789, 666])
    assert c.size() == 3
    assert len(c.select('f1="haha"')) == 1
    assert len(c.select('f1="haha"', valid_ids=[123, 456])) == 1
    assert len(c.select('f1="haha"', valid_ids=[666, 456])) == 0
    c.delete_index()


def test_kv():
    c = KVIndex(WORK_DIR, 'test_kv', {})
    c.delete_index()
    c.create_index()
    c.set(1, '123')
    c.set(2, '456')
    c.set(3, '789')
    assert c.size() == 3
    c.delete(3)
    assert c.size() == 2
    assert c.get(1) == '123'
    assert len(c.sample(2)) == 2
    c.delete_index()


def test_bm25():
    i = TyBM25Index(WORK_DIR, 'test_bm25', {})
    i.delete_index()
    i.create_index()
    i.add('I am from China. My name is Tony.', 1)
    i.add('我 来 自 北京，叫做 赵天成', 2)
    i.add('我 来 自 北京，叫做 赵天成', 3)
    assert i.size() == 3
    assert len(i.rank('name', 10)) == 1
    assert len(i.rank('我', 10)) == 2
    i.delete(1)
    assert i.size() == 2
    assert len(i.rank('name', 10)) == 0
    i.delete_index()


def test_vector():
    index = FaissVectorIndex(WORK_DIR, 'test_vector', {'num_dim': 3})
    index.delete_index()
    index.create_index()
    index.add([1, 2, 3], 1)
    index.add([3, 2, 1], 2)
    index.add([[1, 2, 3], [4, 5, 6]], [3, 4])
    assert index.size() == 4
    assert index.rank([1, 2, 3], 10)[0][0] == pytest.approx(1.0)
    index.delete([1])
    assert index.size() == 3
    index.delete_index()


def test_inverted():
    i = EsInvertedIndex(WORK_DIR, 'test_es_iv', {})
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