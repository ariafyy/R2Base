from typing import Union, List


class Indexer(object):
    def __init__(self):
        self.indices = {}

    def create_index(self, name: str, mapping: dict):
        self.indices[name] = {'mapping': mapping}

    def list_index(self):
        return list(self.indices.keys())

    def delete_index(self, name: str):
        self.indices.pop(name, None)

    def read_index(self, name: str):
        return self.indices.get(name)

    def add_doc(self, index, docs: Union[List[dict], dict]):
        pass

    def read_doc(self, index, doc_id):
        pass

    def update_doc(self, index, docs: Union[List[dict], dict]):
        pass

    def delete_docs(self, index, doc_ids: Union[List[str], str]):
        pass
