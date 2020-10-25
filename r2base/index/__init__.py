

class IndexType(object):
    CUS_INVERTED = "custom_inverted"
    VECTOR = "vector"
    KEYWORD = "keyword"
    BM25 = "bm25"


class BaseIndex(object):
    type: str
