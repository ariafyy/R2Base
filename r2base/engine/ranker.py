from r2base.engine.bases import EngineBase
import logging
from typing import Dict, Union, List


class Ranker(EngineBase):

    logger = logging.getLogger(__name__)

    def get_mapping(self, index_id: str):
        return self.get_index(index_id).get_mappings()

    def size(self, index_id: str) -> int:
        return self.get_index(index_id).size()

    def read_docs(self, index_id: str, doc_ids: Union[str, List[str]]):
        return self.get_index(index_id).read_docs(doc_ids)

    def query(self, index_id: str, q: Dict):
        return self.get_index(index_id).query(q)


