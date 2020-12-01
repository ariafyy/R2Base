from r2base.engine.ranker import Ranker

if __name__ == "__main__":
    ranker = Ranker()
    index = 'wiki'

    while True:
        query = input("Type a query:\n")
        if '|' in query:
            q = query.split('|')[0]
            filter = query.split('|')[1]
        else:
            q = query
            filter = None

        res = ranker.query(index, {'read': {'content': {'q': 'what is the main event?', 'model_id': 'xx'},
                                            'header': {'q': 'what is the date?', 'model_id': 'yy'}},
                                   'match': {'text': q},
                                   'filter': filter,
                                   'size': 5})
        print('{} results'.format(len(res)))
        if len(res) > 0:
            print(res[0]['score'], res[0]['_source']['title'], res[0]['_source'])
        else:
            print("No result")
