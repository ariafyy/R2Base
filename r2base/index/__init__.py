from typing import Dict
import tantivy
import os
import logging


class IndexBase(object):
    type: str

    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str, mapping: Dict):
        self.root_dir = root_dir
        self.index_id = index_id
        self.mapping = mapping
        self.work_dir = os.path.join(self.root_dir, self.index_id)
        self._client = None

    def size(self) -> int:
        pass

    def create_index(self, *args, **kwargs):
        pass

    def add(self, *args, **kwargs):
        pass

    def delete(self, *args, **kwargs):
        pass

    def rank(self, *args, **kwargs):
        pass

    def select(self, *args, **kwargs):
        pass

