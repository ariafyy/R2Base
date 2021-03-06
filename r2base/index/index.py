from r2base import FieldType as FT
from r2base.processors.pipeline import ReducePipeline
from r2base.utils import chunks, get_uid
import os
from typing import Dict, List, Union
from tqdm import tqdm
import logging
import string
from r2base.index.es_full import EsIndex


class Index(object):

    logger = logging.getLogger(__name__)

    def __init__(self, root_dir: str, index_id: str):
        self._validate_index_id(index_id)
        self.index_id = index_id
        self.index_dir = os.path.join(root_dir, self.index_id)
        self._rank_index = None

    def _validate_index_id(self, index_id):
        check = set(string.digits + string.ascii_letters + '_-s')
        for x in index_id:
            if x not in check:
                raise Exception("Invalid index_id. It only contains letters, numbers, and _-.")

    @property
    def mappings(self) -> Dict:
        """
        :return: Load mapping from the disk.
        """
        return self.rank_index.mappings

    @property
    def rank_index(self) -> EsIndex:
        if self._rank_index is None:
            self._rank_index = EsIndex(self.index_dir, self.index_id)

        return self._rank_index

    def create_index(self, mappings: Dict) -> None:
        """
        Normalize the mapping and create index for every sub-index
        :param mappings: mapping of the index
        """
        # check if mapping contain illegal elements
        if FT.ID in mappings.keys():
            raise Exception("{} is reserved. Index creation aborted".format(FT.ID))

        self.logger.info("Creating index {}".format(self.index_id))
        self.rank_index.create_index(mappings)
        return None

    def delete_index(self) -> None:
        """
        :return: delete the whole index from the disk completely
        """
        self.logger.info("Removing index {}".format(self.index_id))

        # first delete each sub-index
        self.rank_index.delete_index()
        self._rank_index = None

    def get_mappings(self) -> Dict:
        # force to reload
        self._mappings = None
        return {k: v.dict() for k, v in self.mappings.items()}

    def size(self) -> int:
        return self.rank_index.size()

    def add_docs(self, docs: Union[Dict, List[Dict]],
                 batch_size: int = 100,
                 show_progress:bool = False) -> List[str]:
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
                doc_id = d.get(FT.ID, get_uid())
                ids.append(doc_id)
                batch_ids.append(doc_id)

            # insert filter fields
            self.rank_index.add(batch, batch_ids)

        return ids

    def delete_docs(self, doc_ids: Union[str, List[str]]) -> Dict:
        # delete the doc from ID index, Filter Index and Every Rank index.
        return self.rank_index.delete(doc_ids)

    def read_docs(self, doc_ids: Union[str, List[str]]) -> List:
        """
        :param doc_ids: read docs give doc IDs
        :return:
        """
        return self.rank_index.read(doc_ids)

    def update_docs(self, docs: Union[Dict, List[Dict]],
                    batch_size: int = 100,
                    show_progress: bool = False
                    ) -> List[str]:
        if type(docs) is dict:
            docs = [docs]
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
        exclude = q.get('exclude', None)
        include = q.get('include', None)
        from_ = q.get('from', 0)

        if top_k <= 0:
            return []

        # Add fields needed for Reduce
        if q_reduce is not None and q_reduce:
            reduce_include = ReducePipeline.get_src_fields(q_reduce)
        else:
            reduce_include = None

        docs = self.rank_index.rank(q_match, q_filter, top_k, include, exclude, reduce_include, from_)

        if q_reduce is not None and q_reduce:
            docs = ReducePipeline().run(q_reduce, docs)

        return docs

    def scroll_query(self, q: Dict):
        print(q)
        q_match = q.get('match', {})
        adv_match = q.get('adv_match', None)

        q_filter = q.get('filter', None)
        batch_size = q.get('size', 10)
        exclude = q.get('exclude', None)
        include = q.get('include', None)
        sort_index = q.get('sort', None)
        search_after = q.get('search_after', None)

        if batch_size <= 0:
            return [], None

        docs, last_id = self.rank_index.scroll(q_match, q_filter, batch_size,
                                               adv_match=adv_match,
                                               includes=include, excludes=exclude,
                                               sort=sort_index, search_after=search_after)

        return docs, last_id

    def delete_query(self, q: Dict):
        q_filter = q['filter']
        return self.rank_index.delete_by_query(q_filter)
