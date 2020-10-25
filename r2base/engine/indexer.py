from r2base.index.keyvalue import KeyValueIndex, KeyValueRankIndex
from r2base.index.vector import VectorIndex
from r2base.index.inverted import InvertedIndex, BM25Index
from r2base.engine.bases import EngineBase
from r2base.engine.bases import FieldType as FT
from r2base.index import IndexType as IT
from r2base.processors.pipeline import Pipeline
import uuid
import logging


class Indexer(EngineBase):

    logger = logging.getLogger(__name__)

    def __init__(self):
        pass

    def create_index(self, index_id, mappings):
        _index = {FT.id: KeyValueIndex(self._sub_index(index_id, FT.id)),
                  'mappings': mappings}

        # TODO: add eval order
        for field, mapping in mappings.items():
            sub_id = self._sub_index(index_id, FT.id)
            if field != FT.id:
                if mapping['type'] == FT.keyword:
                    _index[field] = KeyValueRankIndex(sub_id)

                elif mapping['type'] == FT.vector:
                    _index[field] = VectorIndex(sub_id, mapping['num_dim'])

                elif mapping['type'] == FT.text:
                    if mapping['index'] == IT.CUS_INVERTED:
                        _index[field] = InvertedIndex(sub_id)

                    elif mapping['index'] == IT.BM25:
                        _index[field] = BM25Index(sub_id)

                    elif mapping.index == IT.VECTOR:
                        _index[field] = VectorIndex(sub_id, mapping.num_dim)
                else:
                    _index[field] = None
            else:
                print("_id is reserved for internal indexing.")

        self._dump_index(index_id, _index)
        return True

    def add_doc(self, index_id, docs):
        if type(docs) is not list:
            docs = [docs]

        _index, mappings = self._load_index(index_id)

        ids = []
        for d in docs:
            if FT.id not in d:
                d[FT.id] = str(uuid.uuid4())

            ids.append(d[FT.id])
            _index[FT.id].set(d[FT.id], d)

            for field, value in d.items():
                if field == FT.id:
                    continue

                if field not in _index:
                    self.logger.warning("{} is not defined in mapping".format(field))
                    continue

                if type(mappings[field]['type']) == FT.keyword:
                    _index[field].add(value, d[FT.id])

                elif type(mappings[field]['type']) == FT.vector:
                    _index[field].add(value, d[FT.id])

                elif type(mappings[field]['type']) == FT.text:
                    pipe = Pipeline(mappings[field]['processor'])
                    anno_value = pipe.run(value)

                    # run encoders or NLP processors
                    if type(_index[field]) is VectorIndex:
                        _index[field].add(anno_value, d[FT.id])

                    elif type(_index[field]) is InvertedIndex:
                        _index[field].add(anno_value, d[FT.id])

                    elif type(_index[field]) is BM25Index:
                        _index[field].add(anno_value, d[FT.id])

        self._dump_index(index_id, _index)
        return ids




