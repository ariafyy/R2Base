from typing import List
from pydantic import BaseModel
from r2base.config import EnvVar


class IndexPayload(BaseModel):
    doc: str
    form: list = []


def payload_to_list(hpp: IndexPayload) -> List:
    return [ hpp.doc]
