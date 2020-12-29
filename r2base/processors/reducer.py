from typing import Dict, Union, List
from r2base.processors import processor_map
import numpy as np


class Reducer(object):
    def run(self, query: Dict, data: List):
        for dest_f, pack in query.items():
            src_field = pack['input']
            kwargs = pack.get('kwargs', {})
            p = processor_map[pack['method']]
            input_data = []
            for d_id, d in enumerate(data):
                input_data.append(d['_source'][src_field])

            input_data = np.array(input_data)

            res = p.run(input_data, **kwargs)
            for idx, d in enumerate(data):
                if '_reduce' not in d:
                    d['_reduce'] = {}
                d['_reduce'][dest_f] = res[idx].tolist()

        return data


if __name__ == '__main__':
    query = {
        'f1': {
            'method': 'umap',
            'input': 'f2',
            'kwargs': {}
        }
    }
    data_size = 100
    data = []
    for i in range(data_size):
        data.append({'_source': {'f2': np.random.random(10).tolist()}})

    ans = Reducer().run(query, data)
    for a in ans:
        print(a['_reduce'])