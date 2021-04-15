import requests
import os

host_url = 'http://localhost:8000'

if __name__ == "__main__":
    index = 'media'

    while True:
        query = input("Type a query:\n")
        if '|' in query:
            q = query.split('|')[0]
            filter = query.split('|')[1]
        else:
            q = query
            filter = None

        body = {'match': {'text': q},
                'read': {'field': 'text', 'query': q, 'top_k': 10,
                         'model_id': "distil-roberta-wwm-ext-cmrc+drcd-T4tiny",
                         'model_url': 'https://zk-api.linker.cc/predictor/v1/mrc'},
                'filter': filter,
                'size': 5,
                "highlight": {
                    "pre_tags": ["<b>"],
                    "post_tags": ["</b>"],
                    "fields": {
                        "text": {}
                    }
                }
                }

        raw_res = requests.post(os.path.join(host_url, 'r2base/v1/search/{}/query'.format(index)), json={'query': body})
        raw_res = raw_res.json()
        import pprint
        pprint.pprint (raw_res)
        res = raw_res['ranks']
        if len(res) > 0:
            print(res[0]['score'], res[0]['_source']['title'], res[0]['_source'])
        else:
            print("No result")

        res = raw_res['reads']
        if len(res) > 0:
            print(res[0]['value'], len(res[0]['docs']))
        else:
            print("No reading result")