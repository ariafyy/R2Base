import os
import pickle as pkl
from r2base.config import EnvVar
import logging


class EngineBase(object):

    logger = logging.getLogger(__name__)
    root_dir = os.path.dirname(os.path.abspath(__file__)).replace('r2base/engine', '')
    index_dir = os.path.join(root_dir, EnvVar.INDEX_DIR)
    if not os.path.exists(index_dir):
        logger.info("Created a new index dir {}".format(index_dir))
        os.mkdir(index_dir)

    def _sub_index(self, index_id, field):
        return '{}-{}'.format(index_id, field)

    def _load_index(self, index_id):
        if not os.path.exists(index_id):
            raise Exception("Index {} does not exist.".format(index_id))

        _index = pkl.load(open(index_id, 'rb'))
        mappings = _index['mappings']
        return _index, mappings

    def _dump_index(self, index_id, _index):
        return pkl.dump(_index, open(index_id, 'wb'))
