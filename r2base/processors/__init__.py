from r2base.processors.crafters.tokenizer import NlpTokenizer

processor_map = {
    'tokenize': NlpTokenizer(),
    'wordpiece': None,
    'lower': None,
    'remove_num': None,
    'tokenize_stem': None,
    'onnx_encoder': None,
    'py_encoder': None
}


class ProcessorBase(object):
    def run(self, data):
        raise NotImplementedError