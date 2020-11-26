from typing import List
from pydantic import BaseModel
from typing import Dict


class WriteIndexBody(BaseModel):
    mapping: Dict


class WriteDocBody(BaseModel):
    docs: List = []
    batch_size: int = 100


class SearchBody(BaseModel):
    query: Dict = {}