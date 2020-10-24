from r2base.engine.ranker import RankBase
from r2base.engine.indxer import Indexer
from r2base.index.compound import CompoundIndex


class Pipeline(object):
    def __init__(self):
        self.ranker = RankBase()
        self.indexer = Indexer()
        self.index = CompoundIndex()

    def create_index(self, index, mappings):
        return self.index.create_index(index, mappings)

    def add_doc(self, index, docs):
        # craft docs first using encoders and crafters
        return self.index.add_doc(index, docs)

    def read_doc(self, index, _id):
        return self.index.read_doc(index, _id)

    def rank(self, index, q):
        return self.index.query(index, q)



