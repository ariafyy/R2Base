from r2base.engine.ranker import Ranker
from r2base.engine.reader import Reader
import synonyms
import jieba_fast as jieba

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

        if q:
            tokens = jieba.lcut(q)
            expanded = set()
            for t in tokens:
                ws, scores = synonyms.nearby(t, 5)
                for w, s in zip(ws, scores):
                    if w.strip() and s > 0.8 and w != t:
                        expanded.add(w)

            q = q + '@'.join(list(expanded))

        res = ranker.query(index, {'match': {'text': q, 'title': q},
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
