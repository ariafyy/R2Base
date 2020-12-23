from typing import Union, List
from r2base import FieldType as FT
from r2base import IndexType as IT
from r2base.index import IndexBase
from r2base.config import EnvVar
from r2base.index.keyvalue import KVIndex
from r2base.index.filter import FilterIndex
from r2base.processors.pipeline import Pipeline
from r2base.utils import chunks, get_uid
import os
from joblib import Parallel, delayed
from typing import Dict, Set
from tqdm import tqdm
import json
import numpy as np
import logging
import shutil

if EnvVar.IV_BACKEND == 'ty':
    from r2base.index.iv.ty_inverted import TyBM25Index
    BM25Index = TyBM25Index
elif EnvVar.IV_BACKEND == 'es':
    from r2base.index.iv.es_inverted import EsBM25Index, EsInvertedIndex
    BM25Index = EsBM25Index
    IvIndex = EsInvertedIndex
else:
    raise Exception("Unknown IV Backend = {}".format(EnvVar.IV_BACKEND))


if EnvVar.ANN_BACKEND == 'faiss':
    from r2base.index.ann.faiss_vector import FaissVectorIndex
    VectorIndex = FaissVectorIndex
elif EnvVar.ANN_BACKEND == 'es':
    from r2base.index.ann.es_vector import EsVectorIndex
    VectorIndex = EsVectorIndex
else:
    raise Exception("Unknown IV Backend = {}".format(EnvVar.IV_BACKEND))

class Index(object):

    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str):
        self.index_id = index_id
        self.index_dir = os.path.join(root_dir, self.index_id)
        self._mappings = None
        self._clients = dict()

    def _sub_index(self, field: str):
        # index-text
        return '{}-{}'.format(self.index_id, field)

    def _is_filter(self, field_type):
        return field_type in {FT.KEYWORD, FT.FLOAT, FT.INT, FT.DATETIME, FT.DATE}

    @property
    def mappings(self):
        """
        :return: Load mapping from the disk.
        """
        if self._mappings is not None:
            return self._mappings

        if not os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            return dict()

        self._mappings = json.load(open(os.path.join(self.index_dir, 'mappings.json'), 'r'))
        return self._mappings

    @property
    def filter_mappings(self):
        """
        :return: Load mapping from the disk.
        """
        return {field:mapping for field, mapping in self.mappings.items() if self._is_filter(mapping['type'])}

    def _dump_mappings(self,  mappings: Dict):
        """
        Save the mapping to the disk
        :param mappings:
        :return:
        """
        if os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            raise Exception("Index {} already existed".format(self.index_id))

        return json.dump(mappings, open(os.path.join(self.index_dir, 'mappings.json'), 'w'), indent=2)

    def _fuse_results(self, do_filter: bool, ranks: Dict, filters: Set, top_k: int) -> List[Dict]:
        """
        Given ranks and filters to create final top-K list
        :param do_rank: does the query contain non empty match
        :param do_filter: does the query contain non empty filter
        :param ranks: retrieved docs with scores
        :param filters: valid list of doc_ids
        :param top_k: the size of return
        :return:
        """
        if len(ranks) == 0 and len(filters) == 0:
            return []

        if len(ranks) == 0:
            return []

        if do_filter:
            if len(filters) == 0:
                return []
            else:
                filtered_ranks = [(k, v) for k, v in ranks.items() if k in filters]
                filtered_ranks = sorted(filtered_ranks, key=lambda x: x[1], reverse=True)[0:top_k]
                docs = [{'_source': self.id_index.get(_id), 'score': s} for _id, s in filtered_ranks]
        else:
            ranks = [(k, v) for k, v in ranks.items()]
            ranks = sorted(ranks, key=lambda x: x[1], reverse=True)[0:top_k]
            docs = [{'_source': self.id_index.get(_id), 'score': s} for _id, s in ranks]

        return docs

    def _get_sub_index(self, field: str, mapping: Dict) -> IndexBase:
        if field not in self._clients:
            if field == FT.ID:
                self._clients[field] = KVIndex(self.index_dir, self._sub_index(FT.ID), mapping)

            elif field == IT.FILTER:
                self._clients[field] = FilterIndex(self.index_dir, self._sub_index(field), mapping)

            elif mapping['type'] == FT.TEXT and 'index' in mapping:
                sub_id = self._sub_index(field)
                if mapping['index'] == IT.BM25:
                    self._clients[field] = BM25Index(self.index_dir, sub_id, mapping)
                elif mapping['index'] == IT.VECTOR:
                    self._clients[field] = VectorIndex(self.index_dir, sub_id, mapping)
                elif mapping['index'] == IT.INVERTED:
                    self._clients[field] = IvIndex(self.index_dir, sub_id, mapping)

            elif mapping['type'] == FT.VECTOR:
                sub_id = self._sub_index(field)
                self._clients[field] = VectorIndex(self.index_dir, sub_id, mapping)

            elif mapping['type'] == FT.TERM_SCORE:
                sub_id = self._sub_index(field)
                self._clients[field] = IvIndex(self.index_dir, sub_id, mapping)

        return self._clients.get(field)

    @property
    def filter_index(self) -> FilterIndex:
        return self._get_sub_index(IT.FILTER, self.filter_mappings)

    @property
    def id_index(self) -> KVIndex:
        return self._get_sub_index(FT.ID, {'type': FT.ID})

    def create_index(self, mappings: Dict) -> None:
        """
        Normalize the mapping and create index for every sub-index
        :param mappings: mapping of the index
        """
        self.logger.info("Creating index {}".format(self.index_id))
        if not os.path.exists(self.index_dir):
            self.logger.info("Created a new index dir {}".format(self.index_dir))
            os.mkdir(self.index_dir)

        # assign the internal field and overwrite
        mappings[FT.ID] = {'type': FT.ID}

        # add q_processor
        for field, mapping in mappings.items():
            if mapping['type'] == FT.TEXT and 'index' in mapping:
                if 'q_processor' not in mapping:
                    mapping['q_processor'] = mapping['processor']

                if 'q_model_id' not in mapping and 'model_id' in mapping:
                    mapping['q_model_id'] = mapping['model_id']
        # dump the mapping to disk
        self._dump_mappings(mappings)

        # initialize the index
        self.id_index.create_index()
        self.filter_index.create_index()
        for field, mapping in mappings.items():
            if field == FT.ID:
                continue
            if self._is_filter(mapping['type']):
                continue
            s_index = self._get_sub_index(field, mapping)
            if s_index:
                s_index.create_index()

    def delete_index(self) -> None:
        """
        :return: delete the whole index from the disk completely
        """
        self.logger.info("Removing index {}".format(self.index_id))

        # first delete each sub-index
        self.id_index.delete_index()
        self.filter_index.delete_index()

        for field, mapping in self.mappings.items():
            try:
                if (mapping['type'] == FT.TEXT and 'index' in mapping) or \
                        mapping['type'] == FT.VECTOR:
                    self._get_sub_index(field, mapping).delete_index()
            except Exception as e:
                self.logger.error(e)

        # remove the whole folder
        try:
            shutil.rmtree(self.index_dir)
        except Exception as e:
            self.logger.info(e)

    def get_mappings(self) -> Dict:
        return json.loads(json.dumps(self.mappings))

    def size(self) -> int:
        return self.id_index.size()

    def add_docs(self, docs: Union[Dict, List[Dict]],
                 batch_size: int = 100,
                 show_progress:bool = False) -> List[int]:
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
                if FT.ID not in d:
                    d[FT.ID] = get_uid()

                self.id_index.set(d[FT.ID], d)
                ids.append(d[FT.ID])
                batch_ids.append(d[FT.ID])

            # insert filter fields
            self.filter_index.add(batch, batch_ids)

            # insert each field that needs ranking
            for field, mapping in self.mappings.items():
                valid_ids, valid_docs = [], []
                for d in batch:
                    if field not in d:
                        self.logger.info("{} is missing in document".format(field))
                        continue
                    valid_ids.append(d[FT.ID])
                    valid_docs.append(d)

                if mapping['type'] == FT.TEXT and 'index' in mapping:

                    pipe = Pipeline(self.mappings[field]['processor'])
                    kwargs = {'lang': self.mappings[field]['lang']}
                    annos = pipe.run([b_d[field] for b_d in valid_docs], **kwargs)

                    if mapping['index'] == IT.BM25:
                        self._get_sub_index(field, mapping).add(annos, valid_ids)

                    elif mapping['index'] == IT.INVERTED:
                        self._get_sub_index(field, mapping).add(annos, valid_ids)

                elif mapping['type'] == FT.VECTOR:
                    vectors = [b_d[field] for b_d in valid_docs]
                    self._get_sub_index(field, mapping).add(vectors, valid_ids)

                elif mapping['type'] == FT.TERM_SCORE:
                    ts = [b_d[field] for b_d in valid_docs]
                    self._get_sub_index(field, mapping).add(ts, valid_ids)

        return ids

    def delete_docs(self, doc_ids: Union[int, List[int]]) -> None:
        # delete the doc from ID index, Filter Index and Every Rank index.
        self.id_index.delete(doc_ids)
        self.filter_index.delete(doc_ids)

        for field, mapping in self.mappings.items():
            if (mapping['type'] == FT.TEXT and 'index' in mapping) or \
                    mapping['type'] == FT.VECTOR:
                self._get_sub_index(field, mapping).delete(doc_ids)

    def read_docs(self, doc_ids: Union[int, List[int]]) -> Union[Dict, List]:
        """
        :param doc_ids: read docs give doc IDs
        :return:
        """
        if type(doc_ids) is int:
            return self.id_index.get(doc_ids)
        else:
            return [self.id_index.get(dix) for dix in doc_ids]

    def update_docs(self, docs: Union[Dict, List[Dict]],
                    batch_size: int = 100,
                    show_progress: bool = False
                    ) -> List[int]:
        doc_ids = []
        for d in docs:
            if FT.ID not in docs:
                raise Exception("Cannot update an document that has missing ID")
            doc_ids.append(d[FT.ID])
        self.delete_docs(doc_ids)
        ids = self.add_docs(docs, batch_size, show_progress)
        return ids

    def _field_query(self, mappings, field, value, rank_k):
        mapping = mappings[field]
        if field == FT.ID or self._is_filter(mapping['type']):
            self.logger.warn("Filter or _ID field {} is ignored in match block".format(field))
            return []

        if mapping['type'] == FT.TEXT and 'index' in mapping:
            pipe = Pipeline(mappings[field]['q_processor'])
            kwargs = {'lang': mappings[field]['lang'],
                      'model_id': mappings[field].get('q_model_id')}
            value = pipe.run(value, **kwargs)

        temp = self._get_sub_index(field, mapping).rank(value, rank_k)
        return temp

    def query(self, q: Dict) -> List[Dict]:
        """
        :param index_id:
        :param q: query body
        {
            match: {
                text: xxx,
                vec: {'vector': x, 'text': xxx}
            },
            filter: "f1=XX AND size > 8"
            "size": 10
        }
        :return:
        """
        q_match = q.get('match', {})
        q_filter = q.get('filter', None)
        top_k = q.get('size', 10)
        if top_k <= 0:
            return []

        ranks = dict()
        filters = set()
        do_filter = q_filter is not None

        rank_k = min(10000, top_k*10 if do_filter else top_k)

        if len(q_match) > 0:
            n_job = max(1, min(5, len(q_match)))
            results = Parallel(n_jobs=n_job, prefer="threads")(delayed(self._field_query)(self.mappings, field, value, rank_k)
                                             for field, value in q_match.items())

            for temp in results:
                for score, _id in temp:
                    ranks[_id] = score + ranks.get(_id, 0.0)
        else:
            # get random IDs
            keys = self.id_index.sample(rank_k, return_value=False)
            scores = np.random.random(len(keys))
            ranks = {k: s for k, s in zip(keys, scores)}

        if do_filter:
            filters = self.filter_index.select(q_filter, valid_ids=list(ranks.keys()))

        docs = self._fuse_results(do_filter, ranks, filters, top_k)
        return docs

