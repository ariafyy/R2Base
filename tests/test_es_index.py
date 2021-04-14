from r2base.index.es_full import EsIndex
from r2base.mappings import BasicMapping, VectorMapping, TextMapping, TermScoreMapping
import pytest
import time

WORK_DIR = "."


def test_empty_match():
    mappings = {'f1': {'type': 'keyword'},
                'f2': {'type': 'integer'},
                'f3': {'type': 'float'},
                'f4': {'type': 'date'},
                'f5': {'type': 'datetime'}}

    c = EsIndex(WORK_DIR, 'test_empty_match')

    c.delete_index()
    c.create_index(mappings)
    c.add({'f1': "haha", "f2": 10, "f4": '2019-06-28'}, '1')
    c.add({'f1': "lala", "f3": 10.3, "f5": '2015-01-01T12:10:30Z'}, '2')
    c.add({'f2': 12, "f3": 3.3}, '3')
    c.add([{'f2': 0, "f3": 5.3}, {'f2': 22, "f3": 1.1}], ['4', '5'])
    time.sleep(1)
    assert len(c.rank(None, None, 2)) == 2
    assert len(c.rank(None, None, 3)) == 3
    assert len(c.rank(None, None, 4)) == 4
    assert len(c.rank(None, None, 10)) == 5
    c.delete_index()


def test_filter_index():
    mappings = {'f1': {'type': 'keyword'},
                'f2': {'type': 'integer'},
                'f3': {'type': 'float'},
                'f4': {'type': 'date'},
                'f5': {'type': 'datetime'}}

    c = EsIndex(WORK_DIR, 'test_filter_index')

    c.delete_index()
    c.create_index(mappings)
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


def test_bm25():
    mapping = {'f1': {'type': 'text', 'index': 'bm25', 'lang': 'zh'},
               'f2': {'type': 'text', 'index': 'bm25', 'lang': 'en'},
               }
    i = EsIndex(WORK_DIR, 'test_bm25')
    i.delete_index()
    i.create_index(mapping)
    i.add({'f1': '我来自北京，叫做赵天成'}, '1')
    i.add({'f1': '我来自杭州，叫做赵天成', 'f2': 'I am from New York'}, '2')
    i.add([{'f1': '我来自上海，叫做赵天成', 'f2': 'I am from Los Angeles'},
           {'f1': '我来自深圳，叫做赵天成', 'f2': 'I am from Boston'}],
          ['3', '4'])
    time.sleep(2)
    assert i.size() == 4
    # simple matching
    assert len(i.rank({'f1': '无锡'}, None, 10)) == 0
    assert len(i.rank({'f1': '我'}, None, 10)) == 4
    assert len(i.rank({'f2': 'New'}, None, 10)) == 1
    assert len(i.rank({'f1': '上海', 'f2': 'New'}, None, 10)) == 2

    # advanced query
    assert len(i.rank({'f1': {'value': {'bool': {'must': {'term': {'f1': '来自'}}}}},
                       'f2': 'New'}, None, 10)) == 4

    # delete data
    i.delete(['1', '2'])
    time.sleep(2)

    # search after delete
    assert i.size() == 2
    assert len(i.rank({'f1': '北京'}, None, 10)) == 0
    i.delete_index()


def test_vector():
    mapping = {'f1': {'type': 'vector', 'num_dim': 3},
               'f2': {'type': 'vector', 'num_dim': 3},
               }
    index = EsIndex(WORK_DIR, 'test_vector')
    index.delete_index()
    index.create_index(mapping)
    index.add({'f1': [1, 2, 3]}, '1')
    index.add({'f1': [2, 4, 5], 'f2': [-1, -2, -3]}, '2')
    index.add([{'f1': [3, -2, 3], 'f2': [1, -2, -3]},
               {'f1': [-1, 2, 3], 'f2': [-1, -2, 3]}],
              ['3', '4'])
    time.sleep(3)
    assert index.size() == 4
    assert index.rank({'f1': [1, 2, 3]}, None, 10)[0]['score'] == pytest.approx(1.0)
    assert index.rank({'f1': [2, 4, 5], 'f2': [-1, -2, -3]}, None, 10)[0]['score'] == pytest.approx(2.0)

    # test threshold
    assert len(index.rank({'f1': {'value': [-1, -1, -1], 'threshold': 0.99}}, None, 10)) == 0

    index.delete('1')
    time.sleep(1)
    assert index.size() == 3
    index.delete_index()


def test_term_score():
    mapping = {'f1': {'type': 'term_score', 'mode': 'int'},
               'f2': {'type': 'term_score', 'mode': 'float'},
               }
    i = EsIndex(WORK_DIR, 'test_ts')
    i.delete_index()
    i.create_index(mapping)
    i.add({'f1': {'a': 1, 'b': 2}}, '1')
    i.add([{'f1': {'a': 1, 'b': 1},
            'f2': {'c': 1, 'b': 2}},

           {'f2': {'c': 10, 'd': 4}}
           ],
          ['2', '3'])
    i.add({'f1': {'b': 1, 'd': 2}}, '4')
    time.sleep(2)
    assert i.size() == 4
    docs = i.rank({'f1': ['b']}, None, 10)
    assert len(docs) == 3
    assert docs[0]['score'] == pytest.approx(2.0, rel=0.1)

    # query with threshold
    docs = i.rank({'f1': {'value': ['b'], 'threshold': 3.0}}, None, 10)
    assert len(docs) == 0

    i.delete('1')
    time.sleep(1)
    assert i.size() == 3
    docs = i.rank({'f2': ['a', 'c']}, None, 10)
    assert len(docs) == 2
    assert docs[0]['score'] == pytest.approx(10.0, rel=0.1)
    i.delete_index()
