from r2base.index.filter import FilterIndex
from r2base.index.keyvalue import KVIndex
from r2base.index.inverted import BM25Index

WORK_DIR = "."


def test_filter_index():
    c = FilterIndex(WORK_DIR, 'test_filter_index',
                    {'f1': {'type': "keyword"},
                     'f2': {"type": "integer"},
                     'f3': {"type": "float"}})

    c.delete_index()
    c.create_index()
    c.add({'f1': "haha", "f2": 10}, 123)
    c.add({'f1': "lala", "f3": 10.3}, 456)
    c.add({'f2': 12, "f3": 3.3}, 666)
    c.add([{'f2': 0, "f3": 5.3}, {'f2': 22, "f3": 1.1}],
          [789, 900])

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
    i = BM25Index(WORK_DIR, 'test_bm25', {})
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


