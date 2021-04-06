from r2base.engine.bases import EngineBase
from r2base.config import EnvVar
import logging
from typing import Dict, List
import requests


class Reader(EngineBase):

    logger = logging.getLogger(__name__)

    def read(self, q: Dict, docs: List):
        q_read = q.get("read", {})
        model_id = q_read['model_id']
        query = q_read['query']
        input_k = q_read.get('input_k', 10)
        top_k = q_read.get('top_k', 2)
        params = q_read.get('params', {})
        if not query:
            raise Exception("Query {} is empty".format(query))

        body = {
        "data": [{'q': query, 'doc': d} for d in docs[0:input_k]],
            "model_id": model_id,
            "params": params
        }
        res = requests.post(EnvVar.MRC_URL, json=body)
        res = res.json()['result']
        return res


if __name__ == "__main__":
    doc = "张三是一个铁匠。他很高。"
    q = {'query': '张三是谁？', 'model_id': 'distil-roberta-wwm-ext-cmrc+drcd-T4tiny'}
    x = Reader().read({'read': q}, [doc])
    print(x)