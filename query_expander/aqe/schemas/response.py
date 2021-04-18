from pydantic import BaseModel
from typing import List, Dict


class Response(BaseModel):
    took: float
    terms: Dict = {}