from r2base.engine.ranker import Ranker

if __name__ == "__main__":
    ranker = Ranker()
    index = 'wiki'

    while True:
        query = input("Type a query:\n")
        res = ranker.query(index, {'query': {'text': query}, 'size': 1})
        if len(res) > 0:
            print(res[0]['_source']['_doc_id'], res[0]['_source']['text'][0:50])
        else:
            print("No result")
