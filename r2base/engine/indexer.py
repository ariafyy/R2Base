from r2base.engine.bases import EngineBase
from r2base.config import EnvVar
import logging
from typing import Union, List, Dict
import os


class Indexer(EngineBase):
    logger = logging.getLogger(__name__)

    def create_index(self, index_id: str, mappings: Dict):
        return self.get_index(index_id).create_index(mappings)

    def delete_index(self, index_id: str):
        self.get_index(index_id).delete_index()
        self.indexes.pop(index_id, None)

    def get_mapping(self, index_id: str):
        return self.get_index(index_id).get_mappings()

    def size(self, index_id: str) -> int:
        return self.get_index(index_id).size()

    def list(self) -> List[str]:
        res = []
        for f in os.listdir(self.index_dir):
            if os.path.isdir(os.path.join(self.index_dir, f)):
                res.append(f)
        return res

    def add_docs(self, index_id: str,
                 docs: Union[Dict, List[Dict]],
                 batch_size: int = EnvVar.INDEX_BATCH_SIZE,
                 show_progress: bool = False):
        return self.get_index(index_id).add_docs(docs, batch_size, show_progress)

    def read_docs(self, index_id: str, doc_ids: Union[int, List[int]]):
        return self.get_index(index_id).read_docs(doc_ids)

    def delete_docs(self, index_id: str, doc_ids: Union[int, List[int]]):
        return self.get_index(index_id).delete_docs(doc_ids)

    def update_docs(self, index_id: str,
                    docs: Union[Dict, List[Dict]],
                    batch_size: int = EnvVar.INDEX_BATCH_SIZE,
                    show_progress: bool = False):
        return self.get_index(index_id).update_docs(docs, batch_size, show_progress)

    def scroll_docs(self, index_id: str, skip: int, limit: int):
        return self.get_index(index_id).scroll(skip, limit)