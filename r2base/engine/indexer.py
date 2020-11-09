from r2base.engine.bases import EngineBase
import logging
from typing import Union, List, Dict


class Indexer(EngineBase):

    logger = logging.getLogger(__name__)

    def __init__(self):
        self.indexes = dict()

    def create_index(self, index_id: str, mappings: Dict):
        return self.get_index(index_id).create_index(mappings)

    def add_docs(self, index_id: str,
                 docs: Union[Dict, List[Dict]],
                 show_progress:bool = False):
        return self.get_index(index_id).add_docs(docs, show_progress)




