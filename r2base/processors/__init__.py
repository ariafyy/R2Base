from r2base.processors.crafters.tf_tokenizer import TransformerTokenizer
from r2base.processors.encoders.pytorch_encoders import TransformerEncoders
from r2base.processors.crafters.basics import LowerCase, NoNumber, NoOp, Token2Str, SpaceTokenizer

processor_map = {
    'nothing': NoOp(),
    'tokenize': SpaceTokenizer(),
    'tokens2str': Token2Str(),
    'wordpiece': TransformerTokenizer(),
    'lower': LowerCase(),
    'remove_num': NoNumber(),
    'tokenize_stem': None,
    'onnx_encode': None,
    'py_encode': TransformerEncoders()
}


