# from r2base.processors.crafters.tf_tokenizer import TransformerTokenizer
# from r2base.processors.encoders.pytorch_encoders import TransformerEncoders
from r2base.processors.crafters.basics import LowerCase, NoNumber, NoOp, SpaceTokenizer, NlpTokenizer
from r2base.processors.reducers.umap import UMAPReducer
from r2base.processors.reducers.pca import PCAReducer


processor_map = {
    'nothing': NoOp(),
    'tokenize': SpaceTokenizer(),
    'nlp_tokenize': NlpTokenizer(),
    'lower': LowerCase(),
    'remove_num': NoNumber(),
    'onnx_encode': None,
    'umap': UMAPReducer(),
    'pca': PCAReducer()
}


