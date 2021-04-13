from typing import Dict, List
from r2base.processors import processor_map
import numpy as np
import logging


class Pipeline(object):

    def __init__(self, processors):
        self.processors = processors.split(';')

    def run(self, data, **kwargs):
        res = data
        for p_name in self.processors:
            res = processor_map[p_name].run(res, **kwargs)

        return res


class ReducePipeline(object):
    logger = logging.getLogger(__name__)

    @classmethod
    def get_src_fields(self, query: Dict):
        data = []
        for dest_f, pack in query.items():
            if type(pack) is dict:
                pack = [pack]

            src_field = pack[0]['input']
            data.append(src_field)
        return data

    def run(self, query: Dict, data: List):
        if len(data) < 1:
            self.logger.error("Too few data points ({}) for reduction".format(len(data)))
            return data

        for dest_f, pack in query.items():
            if type(pack) is dict:
                pack = [pack]

            src_field = pack[0]['input']
            input_data = []
            valid_id_map = dict()

            for d_id, d in enumerate(data):
                try:
                    input_data.append(d['_source'][src_field])
                    valid_id_map[d_id] = len(input_data)-1
                except Exception as e:
                    self.logger.error("Bad data skipped for dim reduction")
                    self.logger.error(e)

            input_data = np.array(input_data)
            output_data = input_data

            model_idx = None
            for op_id, op in enumerate(pack):
                kwargs = op.get('kwargs', {})
                p = processor_map[op['method']]
                output_data, model_idx = p.run(input_data, **kwargs)
                input_data = output_data

            for idx, d in enumerate(data):
                if '_reduce' not in d:
                    d['_reduce'] = {}
                if idx in valid_id_map:
                    d['_reduce'][dest_f] = output_data[valid_id_map[idx]].tolist()
                d['_reduce']['model_id'] = model_idx
                d['_source'].pop(src_field)

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