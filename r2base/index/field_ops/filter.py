from r2base.index.util_bases import FieldOpBase
from r2base.mappings import BasicMapping
from r2base import FieldType as FT


class FilterField(FieldOpBase):

    @classmethod
    def to_mapping(cls, mapping: BasicMapping):
        if mapping.type in {FT.DATE, FT.DATETIME}:
            return {'type': 'date'}
        elif mapping.type == FT.INT:
            return {'type': 'integer'}
        elif mapping.type == FT.FLOAT:
            return {'type': 'float'}
        elif mapping.type == FT.KEYWORD:
            return {'type': 'keyword'}

    @classmethod
    def to_add_body(cls, mapping: BasicMapping, value):
        return value
