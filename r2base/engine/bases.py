import os
import pickle as pkl


class EngineBase(object):
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
