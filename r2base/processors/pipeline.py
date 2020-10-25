from r2base.processors import processor_map


class Pipeline(object):

    def __init__(self, processors):
        self.processors = processors.split(',')

    def run(self, data, **kwargs):
        res = data
        for p_name in self.processors:
            res = processor_map[p_name].run(res, **kwargs)

        return res

