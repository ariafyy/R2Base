from r2base.index.index_bases import FieldOpBase
from r2base.mappings import BasicMapping
from typing import Dict, Optional
import logging


class ObjectField(FieldOpBase):
    logger = logging.getLogger(__name__)

    @classmethod
    def to_mapping(cls, mapping: BasicMapping):
        return {'type': 'object', 'enabled': False}

    @classmethod
    def to_add_body(cls, mapping: BasicMapping, value: str):
        return value

    @classmethod
    def to_query_body(cls, *args, **kwargs):
        return None
