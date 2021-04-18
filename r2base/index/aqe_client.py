import requests
import jieba_fast.posseg as pseg
import bisect
from r2base.config import EnvVar
import logging
import os

class AqeClient(object):
    logger = logging.getLogger(__name__)
    root_dir = os.path.dirname(os.path.abspath(__file__)).replace('r2base/index', '')
    with open(os.path.join(root_dir, 'configs', 'stopwords.txt')) as f:
        stop_words = set(l.strip() for l in f.readlines())

    @classmethod
    def _predict(cls, query, top_k, threshold, lang, pos_str):
        if pos_str is None or not pos_str:
            valid_pos = set(EnvVar.AQE_POS.split(','))
        else:
            valid_pos = set(pos_str.split(','))

        tokens = pseg.lcut(query)
        cls.logger.info(tokens)
        valid_tokens = [t for t, pos in tokens if pos in valid_pos and t not in cls.stop_words]
        print(valid_tokens)
        req_url = EnvVar.AQE_URL+'?data={}&top_k={}&threshold={}&lang={}'.format(
            ','.join(valid_tokens), top_k, threshold, lang)

        res = requests.get(req_url)
        return res.json()

    @classmethod
    def expand(cls, aqe, query, lang):
        if aqe is None:
            return query

        try:
            aqe_th = aqe.get('threshold', 0.75)
            top_k = aqe.get('top_k', 10)
            limit = aqe.get('limit', 10)
            pos_str = aqe.get('pos_str', None)
            aqe_res = cls._predict(query, top_k, aqe_th, lang, pos_str)
            terms = []
            for k, ts in aqe_res['terms'].items():
                for t, s in ts:
                    bisect.insort(terms, (s*-1.0, t))
            terms = terms[0:limit]
            terms = [t for s, t in terms]
            cls.logger.info("Found {} new terms".format(len(terms)))
            query = query + ' ' + ' '.join(list(terms))
            query = query.strip()
            cls.logger.info("New query {}".format(query))

        except Exception as e:
            cls.logger.error(e)

        return query



if __name__ == "__main__":
    print(AqeClient.expand({}, '试图将句子最精确地切开，适合文本分析', 'zh'))