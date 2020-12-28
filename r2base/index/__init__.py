from typing import Dict, List, Tuple
import os
import logging


class IndexBase(object):
    type: str

    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str, mapping):
        self.root_dir = root_dir
        self.index_id = index_id
        self.mapping = mapping
        self.work_dir = os.path.join(self.root_dir, self.index_id)
        self._client = None

    def size(self) -> int:
        pass

    def create_index(self, *args, **kwargs) -> None:
        pass

    def delete_index(self, *args, **kwargs) -> None:
        pass

    def add(self, *args, **kwargs) -> None:
        pass

    def delete(self, *args, **kwargs) -> None:
        pass

    def rank(self, *args, **kwargs) -> List[Tuple[float, int]]:
        pass

    def select(self, *args, **kwargs):
        pass

