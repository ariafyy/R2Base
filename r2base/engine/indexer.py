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
                 batch_size: int = 100,
                 show_progress:bool = False):
        return self.get_index(index_id).add_docs(docs, batch_size, show_progress)

    def read_doccs(self, index_id: str,
                 doc_ids: Union[str, List[str]]):

        return self.get_index(index_id)


