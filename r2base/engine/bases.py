import os
from r2base.config import EnvVar
import logging
from r2base.index.index import Index


class EngineBase(object):

    logger = logging.getLogger(__name__)
    root_dir = os.path.dirname(os.path.abspath(__file__)).replace('r2base/engine', '')
    index_dir = os.path.join(root_dir, EnvVar.INDEX_DIR)
    if not os.path.exists(index_dir):
        logger.info("Created a new index dir {}".format(index_dir))
        os.mkdir(index_dir)

    indexes = dict()

    def sync_index(self, index_id: str) -> None:
        if not os.path.exists(os.path.join(self.index_dir, index_id)):
            self.indexes.pop(index_id, None)

    def get_index(self, index_id: str, upsert=False) -> Index:
        # self.sync_index(index_id)

        if index_id not in self.indexes and upsert:
            if len(self.indexes) > EnvVar.MAX_NUM_INDEX:
                self.indexes.pop(list(self.indexes.keys())[0])

            self.indexes[index_id] = Index(self.index_dir, index_id)

        return self.indexes[index_id]



