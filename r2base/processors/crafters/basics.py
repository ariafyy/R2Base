from r2base.processors.bases import ProcessorBase


class NoOp(ProcessorBase):
    def run(self, data: str, **kwargs):
        return data

class LowerCase(ProcessorBase):
    def run(self, data: str, **kwargs):
        return data.lower()


class NoNumber(ProcessorBase):
    def run(self, data: str, **kwargs):
        return ''.join(filter(lambda x: not x.isdigit(), data))
