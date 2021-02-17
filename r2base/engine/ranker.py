from r2base.engine.bases import EngineBase
import logging
from typing import Dict


class Ranker(EngineBase):

    logger = logging.getLogger(__name__)

    def query(self, index_id: str, q: Dict):
        return self.get_index(index_id).query(q)

    def scroll_query(self, index_id: str, q: Dict):
        return self.get_index(index_id).scroll_query(q)

    def delete_query(self, index_id: str, q: Dict):
        return self.get_index(index_id).delete_query(q)


