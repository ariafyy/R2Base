from r2base.processors.bases import ProcessorBase


class LowerCase(ProcessorBase):
    def run(self, data, **kwargs):
        return data.lower()


class NoNumber(ProcessorBase):
    def run(self, data, **kwargs):
        return ''.join(filter(lambda x: not x.isdigit(), data))
