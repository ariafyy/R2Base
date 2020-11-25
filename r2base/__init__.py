
class ServerType(object):
    indexer = "indexer"
    ranker = "ranker"


class FieldType(object):
    id = '_id'
    keyword = 'keyword'
    float = 'float'
    integer = 'integer'
    vector = "vector"
    text = "text"


class IndexType(object):
    CUS_INVERTED = "custom_inverted"
    VECTOR = "vector"
    FILTER = "filter"
    BM25 = "bm25"
    LOOKUP = "lookup"
