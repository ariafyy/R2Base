from r2base.engine.ranker import Ranker

if __name__ == "__main__":
    ranker = Ranker()
    index = 'wiki'

    while True:
        query = input("Type a query:\n")
        res = ranker.query(index, {'match': {'text': query},  'size': 5})
        if len(res) > 0:
            print(res[0]['score'], res[0]['_source']['title'], res[0]['_source']['text'])
        else:
            print("No result")
