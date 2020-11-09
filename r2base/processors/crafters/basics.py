from r2base.processors.bases import ProcessorBase
from typing import List, Tuple

class NoOp(ProcessorBase):
    def run(self, data: str, **kwargs):
        return data

class LowerCase(ProcessorBase):
    def run(self, data: str, **kwargs):
        return data.lower()


class NoNumber(ProcessorBase):
    def run(self, data: str, **kwargs):
        return ''.join(filter(lambda x: not x.isdigit(), data))


class Token2Str(ProcessorBase):
    def run(self, data: List[str], **kwargs):
        delimiter = kwargs.get('delimiter', ' ')
        return delimiter.join(data)


class Tscore2Str(ProcessorBase):
    def run(self, data: List[Tuple], **kwargs):
        delimiter = kwargs.get('delimiter', ' ')
        multi = 5
        max_rep = 20
        values = []
        for term, score in data[0]:
            values.extend(min(int(score*multi), max_rep) * [term])

        return delimiter.join(values)


if __name__ == '__main__':
    data = [(1.3, 'a'), (0.9, 'b'), (0.4, 'c'), (0.1, 'd')]
    print(Tscore2Str().run(data))