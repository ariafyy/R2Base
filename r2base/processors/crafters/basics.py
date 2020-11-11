from r2base.processors.bases import ProcessorBase
from typing import List, Union


class NoOp(ProcessorBase):
    def run(self, data: Union[List[str], str], **kwargs):
        return data


class LowerCase(ProcessorBase):
    def run(self, data: Union[List[str], str], **kwargs) -> Union[List[str], str]:
        if type(data) is List:
            return [d.lower() for d in data]
        else:
            return data.lower()


class NoNumber(ProcessorBase):
    def run(self, data: Union[List[str], str], **kwargs) -> Union[List[str], str]:
        if type(data) is List:
            return [''.join(filter(lambda x: not x.isdigit(), d)) for d in data]
        else:
            return ''.join(filter(lambda x: not x.isdigit(), data))


class Token2Str(ProcessorBase):
    def run(self, data: Union[List[List[str]], List[str]], **kwargs) -> Union[List[str], str]:
        if len(data) == 0:
            return []
        delimiter = kwargs.get('delimiter', ' ')
        if type(data[0]) is List:
            return [delimiter.join(d) for d in data]
        else:
            return delimiter.join(data)
