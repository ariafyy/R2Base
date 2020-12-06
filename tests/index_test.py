from r2base.index.filter import FilterIndex
from r2base.index.keyvalue import KVIndex


def test_filter_index():
    c = FilterIndex('.', 'test_filter_index',
                    {'f1': {'type': "keyword"},
                     'f2': {"type": "integer"},
                     'f3': {"type": "float"}})

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
    c = KVIndex('.', 'test_kv', {})
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

