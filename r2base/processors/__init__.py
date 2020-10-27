from r2base.processors.crafters.nlp_tokenizer import NlpTokenizer
from r2base.processors.crafters.tf_tokenizer import TransformerTokenizer
from r2base.processors.encoders.pytorch_encoders import TransformerEncoders
from r2base.processors.crafters.basics import LowerCase, NoNumber


processor_map = {
    'tokenize': NlpTokenizer(),
    'wordpiece': TransformerTokenizer(),
    'lower': LowerCase(),
    'remove_num': NoNumber(),
    'tokenize_stem': None,
    'onnx_encode': None,
    'py_encode': TransformerEncoders()
}


