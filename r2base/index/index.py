from typing import Dict, Union, List
from r2base import FieldType as FT
from r2base import IndexType as IT
import json
from r2base.index.inverted import BM25Index
from r2base.index.keyvalue import RedisKVRankIndex, RedisKVIndex
import os
from tqdm import tqdm
import uuid
import logging
from r2base.processors.pipeline import Pipeline


class Index(object):

    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str):
        self.index_id = index_id
        self.index_dir = os.path.join(root_dir, self.index_id)
        self.mappings = None
        self._clients = dict()
        if not os.path.exists(self.index_dir):
            self.logger.info("Created a new index dir {}".format(self.index_dir))
            os.mkdir(self.index_dir)

    def get_sub_index(self, field, mapping):
        if field not in self._clients:
            sub_id = self._sub_index(field)
            if mapping['type'] == FT.id:
                self._clients[field] = RedisKVIndex(self.index_dir, sub_id, mapping)

            elif mapping['type'] == FT.keyword:
                self._clients[field] = RedisKVRankIndex(self.index_dir, sub_id, mapping)

                # elif mapping['type'] == FT.vector:
                #    VectorIndexBase(self.root_dir, sub_id, mapping).create_index()

            elif mapping['type'] == FT.text and 'index' in mapping:
                # if mapping['index'] == IT.CUS_INVERTED:
                #    InvertedIndexBase(self.root_dir, sub_id, mapping).create_index()
                if mapping['index'] == IT.BM25:
                    self._clients[field] = BM25Index(self.index_dir, sub_id, mapping)

        return self._clients.get(field)


    @property
    def id_index(self):
        return RedisKVIndex(self.index_dir, self._sub_index(FT.id), {'type': FT.id})

    def _sub_index(self, field: str):
        return '{}-{}'.format(self.index_id, field)

    def _load_mappings(self):
        if self.mappings is not None:
            return self.mappings

        if not os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            raise Exception("Index {} does not exist.".format(self.index_id))

        self.mappings = json.load(open(os.path.join(self.index_dir, 'mappings.json'), 'r'))
        return self.mappings

    def _dump_mappings(self,  mappings: Dict):
        if os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            raise Exception("Index {} already existed".format(self.index_id))

        return json.dump(mappings, open(os.path.join(self.index_dir, 'mappings.json'), 'w'), indent=2)

    def _fuse_results(self, ranks, filters, top_k):
        if len(ranks) == 0 and len(filters) == 0:
            return []

        if len(filters) > 0:
            if len(ranks) > 0:
                filtered_ranks = [(k, v) for k, v in ranks.items() if k in filters]
                filtered_ranks = sorted(filtered_ranks, key=lambda x: x[1], reverse=True)
                docs = [{'_source': self.id_index.get(_id), 'score': s} for _id, s in filtered_ranks][0:top_k]
            else:
                docs = [{'_source': self.id_index.get(_id), 'score': -1} for _id in filters][0:top_k]
        else:
            if len(ranks) > 0:
                ranks = [(k, v) for k, v in ranks.items()]
                ranks = sorted(ranks, key=lambda x: x[1], reverse=True)
                docs = [{'_source': self.id_index.get(_id), 'score': s} for _id, s in ranks][0:top_k]
            else:
                # random sample some data
                docs = self.id_index.sample(top_k)
                docs = [{'_source': doc, 'score': -1} for doc in docs]

        return docs

    def create_index(self, mappings: Dict):
        # assign the internal field and overwrite
        mappings[FT.id] = {'type': FT.id}

        # add q_processor
        for field, mapping in mappings.items():
            if mapping['type'] == FT.text and 'index' in mapping:
                if 'q_processor' not in mapping:
                    mapping['q_processor'] = mapping['processor']

                if 'q_model_id' not in mapping and 'model_id' in mapping:
                    mapping['q_model_id'] = mapping['model_id']

        # TODO: add eval order
        for field, mapping in mappings.items():
            self.get_sub_index(field, mapping).create_index()

        # dump the mapping to disk
        self._dump_mappings(mappings)
        return True

    def add_docs(self, docs: Union[Dict, List[Dict]], show_progress:bool = False):

        if type(docs) is not list:
            docs = [docs]

        mappings = self._load_mappings()

        ids = []
        for d in tqdm(docs, disable=not show_progress):
            if FT.id not in d:
                d[FT.id] = str(uuid.uuid4())
            doc_id = d[FT.id]

            ids.append(d[FT.id])

            for field, mapping in mappings.items():
                if field not in d:
                    self.logger.info("{} is missing in document".format(field))
                    continue
                value = d[field]

                if field == FT.id:
                    self.get_sub_index(field, mapping).set(doc_id, d)

                elif mapping['type'] == FT.keyword:
                    self.get_sub_index(field, mapping).add(value, doc_id)

                #elif mappings[field]['type'] == FT.vector:
                #    _index[field].add(value, d[FT.id])
                elif mapping['type'] == FT.text and 'index' in mapping:

                    pipe = Pipeline(mappings[field]['processor'])
                    kwargs = {'lang': mappings[field]['lang']}
                    anno_value = pipe.run(value, **kwargs)

                    # run encoders or NLP processors
                    #if type(_index[field]) is VectorIndexBase:
                    #    kwargs = {'model_id': mappings[field]['model_id']}
                    #    anno_value = pipe.run(value, **kwargs)
                    #    _index[field].add(anno_value, d[FT.id])

                    #if type(_index[field]) is InvertedIndexBase:
                    #    kwargs = {'model_id': mappings[field]['model_id'], 'mode': 'tscore'}
                    #    anno_value = pipe.run(value, **kwargs)

                    #   _index[field].add(anno_value[0], d[FT.id])
                    if mapping['index'] == IT.BM25:
                        self.get_sub_index(field, mapping).add(anno_value, doc_id)

        return ids

    def query(self, q: Dict):
        """
        :param index_id:
        :param q: query body
        {
            query: {
                text: xxx,
                time: xxx,
                day: xxx,
                vec: {'vector': x, 'text': xxx}
            },
            "size": 10
        }
        :return:
        """
        q_body = q['query']
        top_k = q.get('size', 10)
        ranks = {}
        filters = set()
        mappings = self._load_mappings()

        for field, value in q_body.items():
            if mappings[field]['type'] == FT.id:
                filters = value if type(value) is list else [value]
                continue
            mapping = mappings[field]
            if mapping['type'] == FT.keyword:
                temp = RedisKVRankIndex(self.index_dir, self._sub_index(field), mapping).rank(value)
                filters = filters.union(temp)

                #elif sub_index.type is IT.VECTOR:
                #    temp = sub_index.rank(value, top_k)
                #    for score, _id in temp:
                #        ranks[_id] = score + ranks.get(_id, 0.0)
            elif mapping['type'] == FT.text and 'index' in mapping:
                pipe = Pipeline(mappings[field]['q_processor'])
                kwargs = {'lang': mappings[field]['lang'],
                          'model_id': mappings[field].get('q_model_id')}
                anno_value = pipe.run(value, **kwargs)

                if mapping['index'] == IT.BM25:
                    temp = BM25Index(self.index_dir, self._sub_index(field), mapping).rank(anno_value, top_k)
                    for score, _id in temp:
                        ranks[_id] = score + ranks.get(_id, 0.0)

                #elif sub_index.type is IT.CUS_INVERTED:
                #    temp = sub_index.rank(anno_value, top_k)
                #    for score, _id in temp:
                #        ranks[_id] = score + ranks.get(_id, 0.0)

                #elif sub_index.type is IT.VECTOR:
                #    temp = sub_index.rank(anno_value, top_k)
                #    for score, _id in temp:
                #        ranks[_id] = score + ranks.get(_id, 0.0)

        docs = self._fuse_results(ranks, filters, top_k)
        return docs

