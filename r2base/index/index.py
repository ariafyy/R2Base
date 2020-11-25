from typing import Dict, Union, List
from r2base import FieldType as FT
from r2base import IndexType as IT
from r2base.index.inverted import BM25Index
from r2base.index.keyvalue import KVIndex
from r2base.index.filter import FilterIndex
from r2base.processors.pipeline import Pipeline
from r2base.utils import chunks
import os
from tqdm import tqdm
import json
import uuid
import logging
import shutil


class Index(object):

    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str):
        self.index_id = index_id
        self.index_dir = os.path.join(root_dir, self.index_id)
        self._mappings = None
        self._clients = dict()
        if not os.path.exists(self.index_dir):
            self.logger.info("Created a new index dir {}".format(self.index_dir))
            os.mkdir(self.index_dir)

    def _sub_index(self, field: str):
        # index-text
        return '{}-{}'.format(self.index_id, field)

    def _is_filter(self, field_type):
        return field_type in {FT.keyword, FT.float, FT.integer}

    @property
    def mappings(self):
        """
        :return: Load mapping from the disk.
        """
        if self._mappings is not None:
            return self._mappings

        if not os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            raise Exception("Index {} does not exist.".format(self.index_id))

        self._mappings = json.load(open(os.path.join(self.index_dir, 'mappings.json'), 'r'))
        return self._mappings

    @property
    def filter_mappings(self):
        """
        :return: Load mapping from the disk.
        """
        return {field:mapping for field, mapping in self.mappings if self._is_filter(mapping['type'])}


    def _dump_mappings(self,  mappings: Dict):
        """
        Save the mapping to the disk
        :param mappings:
        :return:
        """
        if os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            raise Exception("Index {} already existed".format(self.index_id))

        return json.dump(mappings, open(os.path.join(self.index_dir, 'mappings.json'), 'w'), indent=2)

    def _fuse_results(self, ranks, filters, top_k) -> List[Dict]:
        """
        Given ranks and filters to create final top-K list
        :param ranks: retrieved docs with scores
        :param filters: valid list of doc_ids
        :param top_k: the size of return
        :return:
        """
        if len(ranks) == 0 and len(filters) == 0:
            return []

        if len(filters) > 0:
            if len(ranks) > 0:
                filtered_ranks = [(k, v) for k, v in ranks.items() if k in filters]
                filtered_ranks = sorted(filtered_ranks, key=lambda x: x[1], reverse=True)
                docs = [{'_source': self.id_index.get(_id), 'score': s}
                        for _id, s in filtered_ranks][0:top_k]
            else:
                docs = [{'_source': self.id_index.get(_id), 'score': -1}
                        for _id in filters][0:top_k]
        else:
            if len(ranks) > 0:
                ranks = [(k, v) for k, v in ranks.items()]
                ranks = sorted(ranks, key=lambda x: x[1], reverse=True)
                docs = [{'_source': self.id_index.get(_id), 'score': s}
                        for _id, s in ranks][0:top_k]
            else:
                # random sample some data
                docs = self.id_index.sample(top_k)
                docs = [{'_source': doc, 'score': -1} for doc in docs]

        return docs

    def _get_sub_index(self, field: str, mapping: Dict):
        if field not in self._clients:
            if mapping['type'] == FT.id:
                self._clients[field] = KVIndex(self.index_dir, self._sub_index(FT.id), mapping)

            elif field == IT.FILTER:
                self._clients[field] = FilterIndex(self.index_dir, self._sub_index(field), mapping)

            elif mapping['type'] == FT.text and 'index' in mapping:
                sub_id = self._sub_index(field)
                if mapping['index'] == IT.BM25:
                    self._clients[field] = BM25Index(self.index_dir, sub_id, mapping)

        return self._clients.get(field)

    @property
    def filter_index(self) -> FilterIndex:
        return self._get_sub_index(IT.FILTER, self.filter_mappings)

    @property
    def id_index(self) -> KVIndex:
        return self._get_sub_index(FT.id, {'type': FT.id})

    def create_index(self, mappings: Dict):
        """
        Normalize the mapping and create index for every sub-index
        :param mappings: mapping of the index
        """
        self.logger.info("Creating index {}".format(self.index_id))
        # assign the internal field and overwrite
        mappings[FT.id] = {'type': FT.id}

        # add q_processor
        for field, mapping in mappings.items():
            if mapping['type'] == FT.text and 'index' in mapping:
                if 'q_processor' not in mapping:
                    mapping['q_processor'] = mapping['processor']

                if 'q_model_id' not in mapping and 'model_id' in mapping:
                    mapping['q_model_id'] = mapping['model_id']

        for field, mapping in mappings.items():
            self._get_sub_index(field, mapping).create_index()

        # dump the mapping to disk
        self._dump_mappings(mappings)
        return True

    def delete_index(self):
        """
        :return: delete the whole index from the disk completely
        """
        self.logger.info("Removing index {}".format(self.index_id))
        try:
            shutil.rmtree(self.index_dir)
        except Exception as e:
            self.logger.info(e)

    def get_mappings(self):
        return json.loads(json.dumps(self._mappings))

    def size(self):
        return self.id_index.size()

    def add_docs(self, docs: Union[Dict, List[Dict]],
                 batch_size: int = 100,
                 show_progress:bool = False):
        """
        Add docs to all sub indexes in batches
        :param docs: a single doc OR a list of docs (dict)
        :param batch_size: the batch size
        :param show_progress: show progress in terminal
        :return: a list of docs
        """
        if type(docs) is not list:
            docs = [docs]

        # process by columns
        ids = []
        for batch in tqdm(chunks(docs, win_len=batch_size, stride_len=batch_size),
                          total=int(len(docs)/batch_size),
                          disable=not show_progress):
            # Insert raw data by ID. Set UID if it's missing
            batch_ids = []
            for d in batch:
                if FT.id not in d:
                    d[FT.id] = str(uuid.uuid1().int >> 64)

                self.id_index.set(d[FT.id], d)
                ids.append(d[FT.id])
                batch_ids.append(d[FT.id])

            # insert filter fields
            self.filter_index.add(batch, batch_ids)

            # insert each field that needs ranking
            for field, mapping in self.mappings.items():
                valid_ids, valid_docs = [], []
                for d in batch:
                    if field not in d:
                        self.logger.info("{} is missing in document".format(field))
                        continue
                    valid_ids.append(d[FT.id])
                    valid_docs.append(d)

                if mapping['type'] == FT.text and 'index' in mapping:

                    pipe = Pipeline(self.mappings[field]['processor'])
                    kwargs = {'lang': self.mappings[field]['lang']}
                    annos = pipe.run([b_d[field] for b_d in valid_docs], **kwargs)

                    if mapping['index'] == IT.BM25:
                        self._get_sub_index(field, mapping).add(annos, valid_ids)

        return ids

    def delete_docs(self, doc_ids: Union[str, List[str]]):
        pass

    def read_docs(self, doc_ids: Union[str, List[str]]):
        """
        :param doc_ids: read docs give doc IDs
        :return:
        """
        if type(doc_ids) is str:
            return self.id_index.get(doc_ids)
        else:
            return [self.id_index.get(dix) for dix in doc_ids]

    def update_docs(self, docs: Union[Dict, List[Dict]],
                    batch_size: int = 100,
                    show_progress: bool = False
                    ):
        pass

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
        mappings = self.mappings

        for field, value in q_body.items():
            if mappings[field]['type'] == FT.id:
                filters = value if type(value) is list else [value]
                continue
            mapping = mappings[field]
            if mapping['type'] == FT.keyword:
                temp = self._get_sub_index(field, mapping).rank(value)
                filters = filters.union(temp)

            elif mapping['type'] == FT.text and 'index' in mapping:
                pipe = Pipeline(mappings[field]['q_processor'])
                kwargs = {'lang': mappings[field]['lang'],
                          'model_id': mappings[field].get('q_model_id')}
                anno_value = pipe.run(value, **kwargs)

                if mapping['index'] == IT.BM25:
                    temp = self._get_sub_index(field, mapping).rank(anno_value, top_k)
                    for score, _id in temp:
                        ranks[_id] = score + ranks.get(_id, 0.0)

        docs = self._fuse_results(ranks, filters, top_k)
        return docs

