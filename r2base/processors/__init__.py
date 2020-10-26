from r2base.processors.crafters.nlp_tokenizer import NlpTokenizer
from r2base.processors.crafters.basics import LowerCase, NoNumber


processor_map = {
    'tokenize': NlpTokenizer(),
    'wordpiece': None,
    'lower': LowerCase(),
    'remove_num': NoNumber(),
    'tokenize_stem': None,
    'onnx_encoder': None,
    'py_encoder': None
}


