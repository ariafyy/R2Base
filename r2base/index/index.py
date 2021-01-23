from typing import Union, List
from r2base import FieldType as FT
from r2base import IndexType as IT
from r2base.index import IndexBase
from r2base.config import EnvVar
from r2base.index.keyvalue import KVIndex
from r2base.index.filter import FilterIndex
from r2base.mappings import parse_mapping, BasicMapping, TextMapping, TermScoreMapping
from r2base.processors.pipeline import Pipeline, ReducePipeline
from r2base.utils import chunks, get_uid
import os
from joblib import Parallel, delayed
from typing import Dict, Set, Any
from tqdm import tqdm
from collections import defaultdict
import json
import numpy as np
import logging
import shutil
import string


if EnvVar.IV_BACKEND == 'ty':
    from r2base.index.iv.ty_inverted import TyBM25Index
    BM25Index = TyBM25Index
elif EnvVar.IV_BACKEND == 'es':
    from r2base.index.iv.es_inverted import EsBM25Index, EsQuantInvertedIndex, EsInvertedIndex
    BM25Index = EsBM25Index
    IvIndex = EsInvertedIndex
    QuantIvIndex = EsQuantInvertedIndex
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
        self._validate_index_id(index_id)
        self.index_id = index_id
        self.index_dir = os.path.join(root_dir, self.index_id)
        self._mappings = None
        self._clients = dict()

    def _validate_index_id(self, index_id):
        check = set(string.digits + string.ascii_letters + '-_')
        for x in index_id:
            if x not in check:
                raise Exception("Invalid index_id. It only contains letters, numbers, - and _.")

    def _sub_index_id(self, field: str):
        # index-text
        return '{}-{}'.format(self.index_id, field)

    def _is_filter(self, field_type):
        return field_type in FT.FILTER_TYPES

    @property
    def mappings(self) -> Dict:
        """
        :return: Load mapping from the disk.
        """
        if self._mappings is not None:
            return self._mappings

        if not os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            return dict()

        temp = json.load(open(os.path.join(self.index_dir, 'mappings.json'), 'r'))
        self._mappings = dict()
        for key, mapping in temp.items():
            temp = parse_mapping(mapping)
            if temp is None:
                raise Exception("Error in parsing mapping {}".format(key))
            self._mappings[key] = temp

        return self._mappings

    @property
    def filter_mappings(self) -> Dict[str, BasicMapping]:
        """
        :return: Load mapping from the disk.
        """
        return {field: mapping for field, mapping in self.mappings.items() if self._is_filter(mapping.type)}

    def _dump_mappings(self,  mappings: Dict):
        """
        Save the mapping to the disk
        :param mappings:
        :return:
        """
        if os.path.exists(os.path.join(self.index_dir, 'mappings.json')):
            raise Exception("Index {} already existed".format(self.index_id))

        temp = {k: v.dict() for k, v in mappings.items()}
        return json.dump(temp, open(os.path.join(self.index_dir, 'mappings.json'), 'w'), indent=2)

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
                sources = self.id_index.get([_id for _id, s in filtered_ranks])
                scores = [s for _id, s in filtered_ranks]
        else:
            ranks = [(k, v) for k, v in ranks.items()]
            ranks = sorted(ranks, key=lambda x: x[1], reverse=True)[0:top_k]
            sources = self.id_index.get([_id for _id, s in ranks])
            scores = [s for _id, s in ranks]

        docs = [{'_source': src, 'score': score} for src, score in zip(sources, scores)]

        return docs

    def _get_sub_index(self, field: str, mapping) -> IndexBase:
        if field not in self._clients:
            if field == FT.ID:
                self._clients[field] = KVIndex(self.index_dir, self._sub_index_id(FT.ID), mapping)

            elif field == IT.FILTER:
                self._clients[field] = FilterIndex(self.index_dir, self._sub_index_id(field), mapping)

            elif mapping.type == FT.TEXT:
                mapping: TextMapping = mapping
                sub_id = self._sub_index_id(field)
                index_mapping = parse_mapping(mapping.index_mapping)
                if mapping.index == IT.BM25:
                    self._clients[field] = BM25Index(self.index_dir, sub_id, mapping)
                elif mapping.index == IT.VECTOR:
                    self._clients[field] = VectorIndex(self.index_dir, sub_id, index_mapping)
                elif mapping.index == IT.INVERTED:
                    self._clients[field] = IvIndex(self.index_dir, sub_id, index_mapping)

            elif mapping.type == FT.VECTOR:
                sub_id = self._sub_index_id(field)
                self._clients[field] = VectorIndex(self.index_dir, sub_id, mapping)

            elif mapping.type == FT.TERM_SCORE:
                mapping : TermScoreMapping  = mapping
                sub_id = self._sub_index_id(field)
                if mapping.mode == 'float':
                    self._clients[field] = IvIndex(self.index_dir, sub_id, mapping)
                elif mapping.mode == 'int':
                    self._clients[field] = QuantIvIndex(self.index_dir, sub_id, mapping)
                else:
                    raise Exception("Unknown term score mode={}".format(mapping.mode))


        return self._clients.get(field)

    def _fuse_field_ranking(self, results: List):
        ranks = dict()
        must_only_ids = dict()
        for idx, (field_scores, threshold) in enumerate(results):
            if threshold is not None:
                must_only_ids[idx] = set()

            for score, _id in field_scores:
                if threshold is not None:
                    must_only_ids[idx].add(_id)

                ranks[_id] = score + ranks.get(_id, 0.0)

        if len(must_only_ids) > 0:
            temp = None
            for k, v in must_only_ids.items():
                if temp is None:
                    temp = v
                else:
                    if len(temp) == 0:
                        break
                    temp = temp.intersection(v)

            ranks = {k: v for k, v in ranks.items() if k in temp}

        return ranks

    @property
    def filter_index(self) -> FilterIndex:
        return self._get_sub_index(IT.FILTER, self.filter_mappings)

    @property
    def id_index(self) -> KVIndex:
        return self._get_sub_index(FT.ID, BasicMapping(type=FT.ID))

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
        mappings[FT.ID] = BasicMapping(type=FT.ID)

        # validate format by parsing the dicts
        for k, mapping in mappings.items():
            mappings[k] = parse_mapping(mapping)

        # dump the mapping to disk
        self._dump_mappings(mappings)

        # initialize the index
        self.id_index.create_index()
        self.filter_index.create_index()
        for field, mapping in self.mappings.items():
            if field == FT.ID:
                continue
            if self._is_filter(mapping.type):
                continue
            s_index = self._get_sub_index(field, mapping)
            if s_index:
                s_index.create_index()

    def delete_index(self) -> None:
        """
        :return: delete the whole index from the disk completely
        """
        self.logger.info("Removing index {}".format(self.index_id))

        for field, mapping in self.mappings.items():
            if mapping.type in FT.MATCH_TYPES:
                self._get_sub_index(field, mapping).delete_index()

        # first delete each sub-index
        self.id_index.delete_index()
        self.filter_index.delete_index()

        # remove the whole folder
        try:
            shutil.rmtree(self.index_dir)
        except Exception as e:
            self.logger.info(e)

    def get_mappings(self) -> Dict:
        return {k: v.dict() for k, v in self.mappings.items()}

    def size(self) -> int:
        return self.id_index.size()

    def scroll(self, limit:int=200, last_key:int=None):
        return self.id_index.scroll(limit, last_key)

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

                ids.append(d[FT.ID])
                batch_ids.append(d[FT.ID])

            # save raw data
            self.id_index.set(batch_ids, batch)

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

                if mapping.type == FT.TEXT:
                    mapping: TextMapping = mapping
                    pipe = Pipeline(mapping.processor)
                    kwargs = {'lang': mapping.lang, 'is_query': False}
                    annos = pipe.run([b_d[field] for b_d in valid_docs], **kwargs)

                    if mapping.index == IT.BM25:
                        self._get_sub_index(field, mapping).add(annos, valid_ids)

                    elif mapping.index == IT.INVERTED:
                        self._get_sub_index(field, mapping).add(annos, valid_ids)

                elif mapping.type == FT.VECTOR:
                    vectors = [b_d[field] for b_d in valid_docs]
                    self._get_sub_index(field, mapping).add(vectors, valid_ids)

                elif mapping.type== FT.TERM_SCORE:
                    ts = [b_d[field] for b_d in valid_docs]
                    self._get_sub_index(field, mapping).add(ts, valid_ids)

        return ids

    def delete_docs(self, doc_ids: Union[int, List[int]]) -> None:
        # delete the doc from ID index, Filter Index and Every Rank index.
        self.id_index.delete(doc_ids)
        self.filter_index.delete(doc_ids)

        for field, mapping in self.mappings.items():
            if mapping.type in FT.MATCH_TYPES:
                self._get_sub_index(field, mapping).delete(doc_ids)

    def read_docs(self, doc_ids: Union[int, List[int]]) -> Union[Dict, List]:
        """
        :param doc_ids: read docs give doc IDs
        :return:
        """
        return self.id_index.get(doc_ids)

    def update_docs(self, docs: Union[Dict, List[Dict]],
                    batch_size: int = 100,
                    show_progress: bool = False
                    ) -> List[int]:
        doc_ids = []
        for d in docs:
            if FT.ID not in d:
                raise Exception("Cannot update an document that has missing ID")
            doc_ids.append(d[FT.ID])
        self.delete_docs(doc_ids)
        ids = self.add_docs(docs, batch_size, show_progress)
        return ids

    def _query_field(self, mapping, field: str, value: Any, rank_k: int):
        if field == FT.ID or self._is_filter(mapping.type):
            self.logger.warn("Filter or _ID field {} is ignored in match block".format(field))
            return [], None

        if type(value) is dict:
            threshold = value['threshold']
            value = value['value']
        else:
            threshold = None

        if mapping.type == FT.TEXT:
            mapping: TextMapping = mapping
            pipe = Pipeline(mapping.q_processor)
            kwargs = {'lang': mapping.lang, 'is_query': True}
            value = pipe.run(value, **kwargs)

        field_scores = self._get_sub_index(field, mapping).rank(value, rank_k)
        if threshold is not None:
            field_scores = [(score, _id) for score, _id in field_scores if score >= threshold]

        return field_scores, threshold

    def query(self, q: Dict) -> List[Dict]:
        """
        :param index_id:
        :param q: query body
        {
            match: {
                vec2: [1,2,3]
                vec2: {'value': [1,2,3], 'threshold': 0.8}
                vec: {'value': [1,2,3], 'threshold': 0.9}
            },
            filter: "f1=XX AND size > 8"
            "size": 10
        }
        :return:
        """
        q_match = q.get('match', {})
        match_args = q.get('match_args', {})
        q_filter = q.get('filter', None)
        q_reduce = q.get('reduce', {})
        top_k = q.get('size', 10)
        exclude = q.get('exclude', [])
        include = q.get('include', [])

        if top_k <= 0:
            return []

        filters = set()
        do_filter = q_filter is not None

        rank_k = min(10000, top_k*10 if do_filter else top_k)

        if len(q_match) > 0:
            n_job = max(1, min(5, len(q_match)))
            results = Parallel(n_jobs=n_job, prefer="threads")(delayed(self._query_field)(self.mappings[field],
                                                                                          field, value, rank_k)
                                             for field, value in q_match.items())

            ranks = self._fuse_field_ranking(results)
        else:
            # get random IDs
            keys = self.id_index.sample(rank_k, return_value=False, sample_mode=match_args.get('sample_mode', 'fixed'))
            scores = np.random.random(len(keys))
            ranks = {k: s for k, s in zip(keys, scores)}

        if do_filter:
            filters = self.filter_index.select(q_filter, valid_ids=list(ranks.keys()))

        docs = self._fuse_results(do_filter, ranks, filters, top_k)

        if q_reduce is not None and q_reduce:
            docs = ReducePipeline().run(q_reduce, docs)

        # include has higher priority than exclude
        if len(include) > 0:
            include = set(include)
            for d in docs:
                keys = list(d['_source'].keys())
                for field in keys:
                    if field not in include:
                        d['_source'].pop(field, None)
        else:
            if len(exclude) > 0:
                exclude = set(exclude)
                for d in docs:
                    for field in exclude:
                        d['_source'].pop(field, None)

        return docs

