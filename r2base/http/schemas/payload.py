from typing import List
from pydantic import BaseModel
from typing import Dict


class CreateIndexPayload(BaseModel):
    index: str
    mapping: Dict


class AddDocPayload(BaseModel):
    docs: List = []
    index: str
    batch_size: int = 100


class RankReadPayload(BaseModel):
    index: str
    query: Dict = {}