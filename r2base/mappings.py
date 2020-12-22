from types import SimpleNamespace
import json
from r2base import FieldType as FT


def from_string(json_str):
    x = json.loads(json_str, object_hook=lambda d: SimpleNamespace(**d))
    return x


class BaseMapping(object):
    type: str
    def __init__(self, data):
        self.__dict__ = data


class KeywordMapping(BaseMapping):
    def __init__(self, data):
        super().__init__(data)
        assert self.type == FT.KEYWORD


class VectorMapping(BaseMapping):
    num_dim: int = 0

    def __init__(self, data):
        super().__init__(data)
        assert self.type == FT.VECTOR
        assert self.num_dim > 0


class TextMapping(BaseMapping):
    lang: str
    index: str
    model: str
    num_dim: int = 0

    def __init__(self, data):
        super().__init__(data)
        assert self.type == FT.TEXT


if __name__ == "__main__":
    x = KeywordMapping({"type": "keyword"})
    print(x.type)
