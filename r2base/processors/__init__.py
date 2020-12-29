from r2base.processors.crafters.tf_tokenizer import TransformerTokenizer
from r2base.processors.encoders.pytorch_encoders import TransformerEncoders
from r2base.processors.crafters.basics import LowerCase, NoNumber, NoOp, SpaceTokenizer, NlpTokenizer
from r2base.processors.reducers.umap import UMAPReducer

processor_map = {
    'nothing': NoOp(),
    'tokenize': SpaceTokenizer(),
    'nlp_tokenize': NlpTokenizer(),
    'wordpiece': TransformerTokenizer(),
    'lower': LowerCase(),
    'remove_num': NoNumber(),
    'onnx_encode': None,
    'py_encode': TransformerEncoders(),
    'umap': UMAPReducer()
}


