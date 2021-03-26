from r2base.engine.bases import EngineBase
from r2base.config import EnvVar
import logging
from typing import Union, List, Dict
from r2base.index.index_bases import EsBaseIndex


class Indexer(EngineBase):
    logger = logging.getLogger(__name__)

    def create_index(self, index_id: str, mappings: Dict):
        self.manager.put(index_id)
        return self.get_index(index_id).create_index(mappings)

    def delete_index(self, index_id: str):
        self.get_index(index_id).delete_index()
        self.indexes.pop(index_id, None)
        self.manager.pop(index_id)

    def get_mapping(self, index_id: str):
        return self.get_index(index_id).get_mappings()

    def size(self, index_id: str) -> int:
        return self.get_index(index_id).size()

    def list(self) -> List[str]:
        return self.manager.list_indices()

    def add_docs(self, index_id: str,
                 docs: Union[Dict, List[Dict]],
                 batch_size: int = EnvVar.INDEX_BATCH_SIZE,
                 show_progress: bool = False):
        return self.get_index(index_id).add_docs(docs, batch_size, show_progress)

    def read_docs(self, index_id: str, doc_ids: Union[str, List[str]]):
        return self.get_index(index_id).read_docs(doc_ids)

    def delete_docs(self, index_id: str, doc_ids: Union[str, List[str]]):
        return self.get_index(index_id).delete_docs(doc_ids)

    def update_docs(self, index_id: str,
                    docs: Union[Dict, List[Dict]],
                    batch_size: int = EnvVar.INDEX_BATCH_SIZE,
                    show_progress: bool = False):
        return self.get_index(index_id).update_docs(docs, batch_size, show_progress)
