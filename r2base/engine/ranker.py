from r2base.engine.bases import EngineBase
import logging
from typing import Dict


class Ranker(EngineBase):

    logger = logging.getLogger(__name__)

    def read_doc(self, index_id: str, id: str):
        self.get_index(index_id).id_index.get(id)

    def query(self, index_id: str, q: Dict):
        return self.get_index(index_id).query(q)


