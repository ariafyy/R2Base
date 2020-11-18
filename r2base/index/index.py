from typing import Dict, Union, List
from r2base import FieldType as FT
from r2base import IndexType as IT
from r2base.index.inverted import BM25Index
from r2base.index.keyvalue import FilterIndex, KVIndex
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

    def _load_mappings(self):
        """
        :return: Load mapping from the disk.
        """
        if self._mappings is not None:
            return self._mappings

        if not os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            raise Exception("Index {} does not exist.".format(self.index_id))

        self._mappings = json.load(open(os.path.join(self.index_dir, 'mappings.json'), 'r'))
        return self._mappings

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
            sub_id = self._sub_index(field)
            if mapping['type'] == FT.id:
                self._clients[field] = KVIndex(self.index_dir, sub_id, mapping)

            elif mapping['type'] == FT.keyword:
                self._clients[field] = FilterIndex(self.index_dir, sub_id, mapping)

            elif mapping['type'] == FT.text and 'index' in mapping:

                if mapping['index'] == IT.BM25:
                    self._clients[field] = BM25Index(self.index_dir, sub_id, mapping)

        return self._clients.get(field)

    @property
    def id_index(self):
        return KVIndex(self.index_dir, self._sub_index(FT.id), {'type': FT.id})

    def create_index(self, mappings: Dict):
        """
        Normalize the mapping and create index for every sub-index
        :param mappings: mapping of the index
        """

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

        mappings = self._load_mappings()
        # process by columns
        ids = []
        for batch in tqdm(chunks(docs, win_len=batch_size, stride_len=batch_size),
                          total=int(len(docs)/batch_size),
                          disable=not show_progress):
            # set missing ids
            batch_ids = []
            for d in batch:
                if FT.id not in d:
                    #d[FT.id] = uuid.uuid1().int >> 64  #64 bit unsigned int
                    d[FT.id] = str(uuid.uuid1().int >> 64)

                batch_ids.append(d[FT.id])

            # process data column by column
            for field, mapping in mappings.items():
                batch_ids, batch_docs = [], []
                for d in batch:
                    if field not in d:
                        self.logger.info("{} is missing in document".format(field))
                        continue
                    batch_ids.append(d[FT.id])
                    batch_docs.append(d)

                # insert into the index by batch
                if field == FT.id:
                    for b_doc_id, b_d in zip(batch_ids, batch_docs):
                        self._get_sub_index(field, mapping).set(b_doc_id, b_d)

                elif mapping['type'] == FT.keyword:
                    batch_values = [doc[field] for doc in batch_docs]
                    self._get_sub_index(field, mapping).add(batch_values, batch_ids)

                elif mapping['type'] == FT.text and 'index' in mapping:

                    pipe = Pipeline(mappings[field]['processor'])
                    kwargs = {'lang': mappings[field]['lang']}
                    annos = pipe.run([b_d[field] for b_d in batch_docs], **kwargs)

                    if mapping['index'] == IT.BM25:
                        self._get_sub_index(field, mapping).add(annos, batch_ids)

            ids.extend(batch_ids)

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
        mappings = self._load_mappings()

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

