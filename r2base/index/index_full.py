from typing import Union, List
from r2base import FieldType as FT
from r2base import IndexType as IT
from r2base.index.keyvalue import KVIndex
from r2base.mappings import parse_mapping, BasicMapping
from r2base.processors.pipeline import ReducePipeline
from r2base.utils import chunks, get_uid
import os
from joblib import Parallel, delayed
from typing import Dict, Set, Any
from tqdm import tqdm
import json
import logging
import shutil
import string
from r2base.index.es_full import EsIndex


class Index(object):

    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str):
        self._validate_index_id(index_id)
        self.index_id = index_id
        self.index_dir = os.path.join(root_dir, self.index_id)
        self._mappings = None
        self._id_index = None
        self._rank_index = None

    def _reserved_fields(self):
        return {'_id', '_oid'}

    def _validate_index_id(self, index_id):
        check = set(string.digits + string.ascii_letters + '_-s')
        for x in index_id:
            if x not in check:
                raise Exception("Invalid index_id. It only contains letters, numbers, and _-.")

    def _sub_index_id(self, field: str):
        # index-text
        return '{}_{}'.format(self.index_id, field)

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

    @property
    def id_index(self) -> KVIndex:
        if self._id_index is None:
            self._id_index = KVIndex(self.index_dir, self._sub_index_id(FT.ID), BasicMapping(type=FT.ID))
        return self._id_index

    @property
    def rank_index(self) -> EsIndex:
        if self._rank_index is None:
            # filter mappings
            self._rank_index = EsIndex(self.index_dir, self._sub_index_id(IT.RANK), self.mappings)
        return self._rank_index

    def create_index(self, mappings: Dict) -> None:
        """
        Normalize the mapping and create index for every sub-index
        :param mappings: mapping of the index
        """
        # check if mapping contain illegal elements
        for key in mappings.keys():
            if key in self._reserved_fields():
                raise Exception("{} is reserved. Index creation aborted".format(key))

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
        self.rank_index.create_index()
        return None

    def delete_index(self) -> None:
        """
        :return: delete the whole index from the disk completely
        """
        self.logger.info("Removing index {}".format(self.index_id))

        # first delete each sub-index
        self.id_index.delete_index()
        self.rank_index.delete_index()

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
            self.rank_index.add(batch, batch_ids)

        return ids

    def delete_docs(self, doc_ids: Union[int, List[int]]) -> None:
        # delete the doc from ID index, Filter Index and Every Rank index.
        self.id_index.delete(doc_ids)
        self.rank_index.delete(doc_ids)

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
        q_filter = q.get('filter', None)
        q_reduce = q.get('reduce', {})
        top_k = q.get('size', 10)
        exclude = q.get('exclude', [])
        include = q.get('include', [])

        if top_k <= 0:
            return []

        ranks = self.rank_index.rank(q_match, q_filter, top_k)
        sources = self.id_index.get([_id for _id, s in ranks])
        docs = []
        for idx in range(len(ranks)):
            docs.append({'_source': sources[idx], 'score': ranks[idx][1]})

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

    def scroll_query(self, q: Dict):
        q_match = q.get('match', {})
        q_filter = q.get('filter', None)
        top_k = q.get('size', 10)
        exclude = q.get('exclude', [])
        include = q.get('include', [])
        sort_index = q.get('sort', {})
        search_after = q.get('search_after', None)

        if top_k <= 0:
            return [], None

        filters = set()
        do_filter = q_filter is not None
        rank_k = top_k
        q_sort = {'sort': sort_index}

        if search_after:
            q_sort['search_after'] = search_after

        ranks = self.rank_index.rank(q_match, q_filter, top_k)
        sources = self.id_index.get([_id for _id, s in ranks])

        if len(q_match) > 0:
            n_job = max(1, min(5, len(q_match)))
            results = Parallel(n_jobs=n_job, prefer="threads")(delayed(self._query_field)(self.mappings[field],
                                                                                          field, value, rank_k, q_sort)
                                                               for field, value in q_match.items())

            ranks = self._fuse_field_ranking(results)
        else:
            pass

        if do_filter:
            filters = self.filter_index.select(q_filter, valid_ids=list(ranks.keys()))
        # print(results)
        if results[0][0]:
            last_id = results[0][0][-1][-1]
        else:
            last_id = None
        docs = self._fuse_results(do_filter, ranks, filters, top_k)

        return docs, last_id