import requests
import os

host_url = 'http://localhost:8000'

if __name__ == "__main__":
    index = 'wiki-basic'

    while True:
        query = input("Type a query:\n")
        if '|' in query:
            q = query.split('|')[0]
            filter = query.split('|')[1]
        else:
            q = query
            filter = None

        body = {'match': {'text': q},
                'filter': filter,
                'size': 5}
        res = requests.post(os.path.join(host_url, 'r2base/v1/search/wiki-basic/query'), json={'query': body})
        res = res.json()
        print('{} results'.format(len(res['ranks'])))
        res = res['ranks']
        if len(res) > 0:
            print(res[0]['score'], res[0]['_source']['title'], res[0]['_source'])
        else:
            print("No result")
