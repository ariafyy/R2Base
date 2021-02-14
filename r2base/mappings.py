from pydantic import BaseModel
from r2base import FieldType as FT
from typing import Dict, Any


def parse_mapping(mapping: dict):
    """
    ID = '_id'
    KEYWORD = 'keyword'
    FLOAT = 'float'
    INT = 'integer'
    VECTOR = "vector"
    TERM_SCORE = "term_score"
    TEXT = "text"
    DATE = "date"
    DATETIME = "datetime"
    :param mapping:
    :return:
    """
    basic_map: BasicMapping = BasicMapping.parse_obj(mapping)
    if basic_map.type in FT.FILTER_TYPES or basic_map.type == FT.ID:
        return basic_map

    if basic_map.type == FT.VECTOR:
        return VectorMapping.parse_obj(mapping)

    if basic_map.type == FT.TERM_SCORE:
        return TermScoreMapping.parse_obj(mapping)

    if basic_map.type == FT.TEXT:
        return TextMapping.parse_obj(mapping)

    if basic_map.type == FT.META:
        return MetaMapping.parse_obj(mapping)


class BasicMapping(BaseModel):
    type: str


class MetaMapping(BasicMapping):
    value: Any

class VectorMapping(BasicMapping):
    num_dim: int


class TermScoreMapping(BasicMapping):
    mode: str = 'float'


class TextMapping(BasicMapping):
    lang: str
    index: str
    index_mapping: Dict = dict()
    processor: str = None
    q_processor: str = None

    def __init__(self, **data):
        print(data)
        # initialize default values here for different index types
        if 'processor' not in data or data['processor'] == 'nothing':
            if data['lang'] == 'zh':
                data['processor'] = 'nlp_tokenize'
            else:
                data['processor'] = 'nothing'

        if 'q_processor' not in data:
            data['q_processor'] = data.get('processor')

        if 'index_mapping' not in data:
            data['index_mapping'] = {'type': data['index']}

        super().__init__(**data)


if __name__ == "__main__":
    x = MetaMapping.parse_obj({"type": "text",
                               "value": {
                                   "lang": "zh",
                                   "index": "bm25",
                                   "index_mapping": {"type": "vector", "num_dim": 123}
                               }
                             })
    print(x.dict())
    exit()
    x = TextMapping.parse_obj({"type": "text",
                               "lang": "zh",
                               "index": "bm25",
                               "index_mapping": {"type": "vector", "num_dim": 123}}
                              )
    print(x.dict())

