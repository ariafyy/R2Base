

class FieldType(object):
    ID = '_id'
    KEYWORD = 'keyword'
    FLOAT = 'float'
    INT = 'integer'
    VECTOR = "vector"
    TERM_SCORE = "term_score"
    TEXT = "text"
    META = "meta"
    DATE = "date"
    DATETIME = "datetime"

    FILTER_TYPES = {KEYWORD, FLOAT, INT, DATE, DATETIME}
    MATCH_TYPES = {TERM_SCORE, TEXT, VECTOR}
    ID_TYPES = {ID}


class IndexType(object):
    INVERTED = "inverted"
    VECTOR = "vector"
    FILTER = "filter"
    BM25 = "bm25"
    LOOKUP = "lookup"
    RANK = "rank"
