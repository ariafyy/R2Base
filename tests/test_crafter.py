from r2base.processors.crafters.tf_tokenizer import TransformerTokenizer

WORK_DIR = "."


def test_tf_tokenizer():
    t = TransformerTokenizer()
    model_id = 'bert-base-chinese-zh_v4-10K-ab_to_abab'
    x = t.run('我是一个好人', **{'model_id': model_id})
    assert x == ['我', '是', '一', '个', '好', '人']

    x = t.run('我是一个好人', **{'model_id': model_id, 'mode': 'all'})
    assert x == ['我', '是', '一', '个', '好', '人', ('一', '个'), ('好', '人')]

    x = t.run('我是一个好人', **{'model_id': model_id, 'mode': 'word'})
    assert x == ['我', '是', ('一', '个'), ('好', '人')]


