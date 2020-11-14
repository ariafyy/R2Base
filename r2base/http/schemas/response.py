from pydantic import BaseModel
from typing import List


class SearchResult(BaseModel):
    took: int
    ranks: List = []
    reads: List = []


class DocWriteResult(BaseModel):
    took: int
    doc_ids: List = []


class IndexWriteResult(BaseModel):
    took: int
    index: str