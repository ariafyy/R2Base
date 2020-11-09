
class FieldType(object):
    id = '_id'
    keyword = 'keyword'
    vector = "vector"
    text = "text"


class IndexType(object):
    CUS_INVERTED = "custom_inverted"
    VECTOR = "vector"
    KEYWORD = "keyword"
    BM25 = "bm25"
    MEILI = "meili"