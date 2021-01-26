from r2base.processors.bases import ProcessorBase
from typing import List, Union
import jieba_fast as jieba


class NoOp(ProcessorBase):
    def run(self, data: Union[List[str], str], **kwargs):
        return data


class LowerCase(ProcessorBase):
    def run(self, data: Union[List[str], str], **kwargs) -> Union[List[str], str]:
        if type(data) is list:
            return [d.lower() for d in data]
        else:
            return data.lower()


class NoNumber(ProcessorBase):
    def run(self, data: Union[List[str], str], **kwargs) -> Union[List[str], str]:
        if type(data) is list:
            return [''.join(filter(lambda x: not x.isdigit(), d)) for d in data]
        else:
            return ''.join(filter(lambda x: not x.isdigit(), data))


class SpaceTokenizer(ProcessorBase):
    def run(self, data: Union[List[str], str], **kwargs) -> Union[List[List[str]], List[str]]:
        if type(data) is list:
            return [d.split() for d in data]
        else:
            return data.split()


class NlpTokenizer(ProcessorBase):
    def run(self, data: Union[List[str], str], **kwargs) -> Union[List[str], str]:
        lang = kwargs['lang']
        is_query = kwargs.get('is_query', False)

        if lang.lower() != 'zh':
            return data

        if type(data) is list:
            res = []
            for d in data:
                tokens = jieba.lcut(d) if is_query else jieba.lcut_for_search(d)
                res.append('/'.join(tokens))
            return res
        else:
            tokens = jieba.lcut(data) if is_query else jieba.lcut_for_search(data)
            return '/'.join(tokens)
