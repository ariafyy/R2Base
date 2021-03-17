from r2base.processors.crafters.tf_tokenizer import TransformerTokenizer
from r2base.processors.crafters.basics import LowerCase, NoNumber, NoOp, SpaceTokenizer, NlpTokenizer
from r2base.processors.reducers.pca import PCAReducer
from r2base.config import EnvVar
import logging

logger = logging.getLogger()

if EnvVar.UMAP_BACKEND == 'gpu':
    logger.info("USE CUDA UMAP")
    from r2base.processors.reducers.gpu_umap import GpuUMAPReducer as UMAPReducer
else:
    from r2base.processors.reducers.umap import UMAPReducer

processor_map = {
    'nothing': NoOp(),
    'tokenize': SpaceTokenizer(),
    'nlp_tokenize': NlpTokenizer(),
    'tf_tokenize': TransformerTokenizer(),
    'lower': LowerCase(),
    'remove_num': NoNumber(),
    'onnx_encode': None,
    'umap': UMAPReducer(),
    'pca': PCAReducer()
}


