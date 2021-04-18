import requests
import jieba_fast.posseg as pseg
import bisect
from r2base.config import EnvVar
import logging

class AqeClient(object):
    logger = logging.getLogger(__name__)
    valid_pos = set(EnvVar.AQE_POS.split(','))

    @classmethod
    def _predict(cls, query, top_k, threshold, lang):
        tokens = pseg.lcut(query)
        print(tokens)
        cls.logger.info(tokens)
        valid_tokens = [t for t, pos in tokens if pos in cls.valid_pos]
        req_url = EnvVar.AQE_URL+'?data={}&top_k={}&threshold={}&lang={}'.format(
            ','.join(valid_tokens), top_k, threshold, lang)

        res = requests.get(req_url)
        return res.json()

    @classmethod
    def expand(cls, aqe, query, lang):
        if aqe is None:
            return query

        try:
            aqe_th = aqe.get('threshold', 0.8)
            top_k = aqe.get('top_k', 10)
            limit = aqe.get('limit', 10)
            aqe_res = cls._predict(query, top_k, aqe_th, lang)
            terms = []
            for k, ts in aqe_res['terms'].items():
                for t, s in ts:
                    bisect.insort(terms, (s*-1.0, t))
            terms = terms[0:limit]
            terms = [t for s, t in terms]
            cls.logger.info("Found {} new terms".format(len(terms)))
            query = query + ' '.join(list(terms))

        except Exception as e:
            cls.logger.error(e)

        return query



if __name__ == "__main__":
    print(AqeClient.expand({}, '试图将句子最精确地切开，适合文本分析', 'zh'))