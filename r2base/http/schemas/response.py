from pydantic import BaseModel
from typing import List, Dict, Union


class Search(BaseModel):
    took: int
    ranks: List = []
    reads: List = []


class ScrollSearch(BaseModel):
    took: int
    ranks: List = []
    reads: List = []
    last_id: Union[int, None]

class DocWrite(BaseModel):
    took: int
    doc_ids: List[int]
    action: str


class DocRead(BaseModel):
    docs: Union[List, Dict, None]


class ScrollRead(BaseModel):
    docs: List
    last_key: int


class IndexWrite(BaseModel):
    index: str
    action: str


class IndexRead(BaseModel):
    index: str
    size: int


class IndexList(BaseModel):
    indexes: List


class MappingRead(BaseModel):
    index: str
    mappings: Dict
