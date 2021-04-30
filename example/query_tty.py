from r2base.engine.ranker import Ranker
from r2base.engine.reader import Reader
if __name__ == "__main__":
    ranker = Ranker()
    reader = Reader()
    index = 'wiki-zh'

    while True:
        query = input("Type a query:\n")
        if '|' in query:
            q = query.split('|')[0]
            filter = query.split('|')[1]
        else:
            q = query
            filter = None


        res = ranker.query(index, {'match': {'text': {'value': q, 'weight': 0.1, 'required': True},
                                             'title': {'value': q, 'weight': 0.2}},
                                   'highlight': {'fields': {'text': {}, 'title': {}}},
                                   'filter': filter,
                                   'size': 5})

        r_q = {'read': {'field': 'text', 'query': q, 'top_k': 10,
                         'model_id': "distil-roberta-wwm-ext-cmrc+drcd-T4tiny",
                         'model_url': 'https://zk-api.linker.cc/predictor/v1/mrc'}}

        ress = reader.read(r_q, res)
        print('{} results'.format(len(res)))
        if len(res) > 0:
            print(res[0]['score'], res[0]['_source']['title'], res[0]['_source'])
        else:
            print("No result")

        print(ress)
