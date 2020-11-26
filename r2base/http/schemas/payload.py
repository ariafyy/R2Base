from typing import List
from pydantic import BaseModel
from typing import Dict


class CreateIndexBody(BaseModel):
    mapping: Dict


class AddDocBody(BaseModel):
    docs: List = []
    batch_size: int = 100

class RankReadBody(BaseModel):
    query: Dict = {}