from r2base.engine.bases import EngineBase
import logging
from typing import Union, List, Dict


class Indexer(EngineBase):
    logger = logging.getLogger(__name__)

    def __init__(self):
        self.indexes = dict()

    def create_index(self, index_id: str, mappings: Dict):
        return self.get_index(index_id).create_index(mappings)

    def delete_index(self, index_id: str):
        self.get_index(index_id).delete_index()
        self.indexes.pop(index_id, None)

    def get_mapping(self, index_id: str):
        return self.get_index(index_id).get_mappings()

    def size(self, index_id: str) -> int:
        return self.get_index(index_id).size()

    def add_docs(self, index_id: str,
                 docs: Union[Dict, List[Dict]],
                 batch_size: int = 100,
                 show_progress: bool = False):
        return self.get_index(index_id).add_docs(docs, batch_size, show_progress)

    def read_docs(self, index_id: str, doc_ids: Union[str, List[str]]):
        return self.get_index(index_id).read_docs(doc_ids)

    def delete_docs(self, index_id: str, doc_ids: Union[str, List[str]]):
        return self.get_index(index_id).read_docs(doc_ids)

    def update_docs(self, index_id: str,
                    docs: Union[Dict, List[Dict]],
                    batch_size: int = 100,
                    show_progress: bool = False):
        return self.get_index(index_id).update_docs(docs, batch_size, show_progress)
