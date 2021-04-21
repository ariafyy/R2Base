from r2base.engine.bases import EngineBase
from r2base.config import EnvVar
import logging
from typing import Dict, List
import requests


class Reader(EngineBase):

    logger = logging.getLogger(__name__)

    def read(self, q: Dict, docs: List):
        q_read = q.get("read", {})
        if not q_read or not docs:
            return []

        model_id = q_read['model_id']
        field = q_read['field']
        query = q_read['query']
        top_k = q_read.get('top_k', 10)
        params = q_read.get('params', {})
        threshold = q_read.get('threshold', 0.5)
        model_url = q_read.get('model_url', EnvVar.MRC_URL)

        if not query:
            raise Exception("Query {} is empty".format(query))

        body = {
            "data": [{'q': query, 'doc': d['_source'][field][0:500]} for d in docs[0:top_k]],
            "model_id": model_id,
            "params": params
        }
        res = requests.post(model_url, json=body)
        res = res.json()['result']

        # add ranker score (filter out empty values)
        new_res = []
        for a_id, ans in enumerate(res):
            if ans.get('missing_warning') or ans.get('prob', 0.0) < threshold:
                continue
            ans['ranker_score'] = docs[a_id]['score']
            ans['combo_score'] = ans['ranker_score'] + ans.get('score')
            ans['_uid'] = docs[a_id]['_source']['_uid']
            new_res.append(ans)

        # rerank
        new_res = sorted(new_res, key=lambda x: x['combo_score'], reverse=True)

        # merge same answer
        used_map = dict()
        merged_ans = []
        for a_id, ans in enumerate(new_res):
            a_v = ans['value']
            if a_v not in used_map:
                used_map[ans['value']] = len(merged_ans)
                merged_ans.append({'value': ans['value'], 'docs': [ans]})
            else:
                merged_ans[used_map[a_v]]['docs'].append(ans)

        return merged_ans


if __name__ == "__main__":
    doc = "张三是一个铁匠。他很高。"
    q = {'query': '张三是谁？',
         'model_id': 'distil-roberta-wwm-ext-cmrc+drcd-T4tiny',
         'model_url': 'https://zk-api.linker.cc/predictor/v1/mrc'}
    x = Reader().read({'read': q}, [doc])
    print(x)