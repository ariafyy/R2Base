

class FieldType(object):
    ID = '_id'
    KEYWORD = 'keyword'
    FLOAT = 'float'
    INT = 'integer'
    VECTOR = "vector"
    TEXT = "text"
    DATE = "date"
    DATETIME = "datetime"


class IndexType(object):
    INVERTED = "inverted"
    VECTOR = "vector"
    FILTER = "filter"
    BM25 = "bm25"
    LOOKUP = "lookup"
