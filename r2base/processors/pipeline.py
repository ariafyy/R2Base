from typing import Dict, Union, List
from r2base.processors import processor_map
import numpy as np


class Pipeline(object):

    def __init__(self, processors):
        self.processors = processors.split(';')

    def run(self, data, **kwargs):
        res = data
        for p_name in self.processors:
            res = processor_map[p_name].run(res, **kwargs)

        return res


class ReducePipeline(object):
    def run(self, query: Dict, data: List):
        for dest_f, pack in query.items():
            if type(pack) is dict:
                pack = [pack]

            src_field = pack[0]['input']
            input_data = []
            for d_id, d in enumerate(data):
                input_data.append(d['_source'][src_field])

            input_data = np.array(input_data)
            output_data = input_data

            for op_id, op in enumerate(pack):
                kwargs = op.get('kwargs', {})
                p = processor_map[op['method']]
                output_data = p.run(input_data, **kwargs)
                input_data = output_data

            for idx, d in enumerate(data):
                if '_reduce' not in d:
                    d['_reduce'] = {}
                d['_reduce'][dest_f] = output_data[idx].tolist()

        return data


if __name__ == '__main__':
    query = {
        'f1': {
            'method': 'pca',
            'input': 'f2',
            'kwargs': {}
        }
    }
    data_size = 100
    data = []
    for i in range(data_size):
        data.append({'_source': {'f2': np.random.random(10).tolist()}})

    ans = ReducePipeline().run(query, data)
    for a in ans:
        print(a['_reduce'])