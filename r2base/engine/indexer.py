from r2base.index.keyvalue import KeyValueIndex, KeyValueRankIndex
from r2base.index.vector import VectorIndex
from r2base.index.inverted import InvertedIndex, BM25Index
from r2base.engine.bases import EngineBase
from r2base import FieldType as FT
from r2base import IndexType as IT
from r2base.processors.pipeline import Pipeline
import uuid
import logging
from tqdm import tqdm
from typing import Union, List, Dict


class Indexer(EngineBase):

    logger = logging.getLogger(__name__)

    def __init__(self):
        pass

    def create_index(self, index_id: str, mappings: dict):
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

                elif mapping['type'] == FT.text and 'index' in mapping:
                    if 'q_processor' not in mapping:
                        mapping['q_processor'] = mapping['processor']

                    if 'q_model_id' not in mapping and 'model_id' in mapping:
                        mapping['q_model_id'] = mapping['model_id']

                    if mapping['index'] == IT.CUS_INVERTED:
                        _index[field] = InvertedIndex(sub_id)

                    elif mapping['index'] == IT.BM25:
                        _index[field] = BM25Index(sub_id)

                    elif mapping['index'] == IT.VECTOR:
                        _index[field] = VectorIndex(sub_id, mapping['num_dim'])
                else:
                    _index[field] = None
            else:
                print("_id is reserved for internal indexing.")

        self._dump_index(index_id, _index)
        return True

    def add_docs(self, index_id: str,
                 docs: Union[Dict, List[Dict]],
                 show_progress:bool = False):

        if type(docs) is not list:
            docs = [docs]

        _index, mappings = self._load_index(index_id)

        ids = []
        for d in tqdm(docs, disable=not show_progress):
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

                if mappings[field]['type'] == FT.keyword:
                    _index[field].add(value, d[FT.id])

                elif mappings[field]['type'] == FT.vector:
                    _index[field].add(value, d[FT.id])

                elif mappings[field]['type'] == FT.text and 'index' in mappings[field]:
                    pipe = Pipeline(mappings[field]['processor'])

                    # run encoders or NLP processors
                    if type(_index[field]) is VectorIndex:
                        kwargs = {'model_id': mappings[field]['model_id']}
                        anno_value = pipe.run(value, **kwargs)

                        _index[field].add(anno_value, d[FT.id])

                    elif type(_index[field]) is InvertedIndex:
                        kwargs = {'model_id': mappings[field]['model_id'], 'mode': 'tscore'}
                        anno_value = pipe.run(value, **kwargs)

                        _index[field].add(anno_value[0], d[FT.id])

                    elif type(_index[field]) is BM25Index:
                        kwargs = {'lang': mappings[field]['lang']}
                        anno_value = pipe.run(value, **kwargs)

                        _index[field].add(anno_value, d[FT.id])

        self._dump_index(index_id, _index)
        return ids




