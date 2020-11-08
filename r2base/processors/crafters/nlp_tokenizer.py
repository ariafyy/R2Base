import stanza
from r2base.processors.bases import ProcessorBase
from r2base.config import EnvVar
import os
import logging


class NlpTokenizer(ProcessorBase):
    models = {}
    root_dir = os.path.dirname(os.path.realpath(__file__)).replace('r2base/processors/crafters', '')
    rsc_dir = os.path.join(root_dir, '{}/stanza'.format(EnvVar.MODEL_DIR))
    logger = logging.getLogger(__name__)
    @classmethod
    def _get_model(cls, lang:str, split_words:bool = True):
        if lang not in cls.models:
            cls.logger.info("Loading tokenizer for {}".format(lang))
            if lang == 'ko' and split_words:
                nlp = stanza.Pipeline(dir=cls.rsc_dir, lang=lang, processors='tokenize,pos,lemma')
            else:
                nlp = stanza.Pipeline(dir=cls.rsc_dir, lang=lang, processors='tokenize')
            cls.models[lang] = nlp

        return cls.models[lang]

    @classmethod
    def tokenize(cls, lang: str, text: str, verbose:bool = False):
        res = cls._get_model(lang)(text)
        tokens = []
        for s in res.sentences:
            sent = []
            for t in s.words:
                if t.lemma is not None:
                    words = t.lemma.split('+')
                    sent.extend(words)
                else:
                    sent.append(t.text)

            tokens.extend(sent)
        if verbose:
            print('{}->{}'.format(text, tokens))
        return tokens

    def run(self, data, lang='en'):
        return self.tokenize(lang, data)


if __name__ == "__main__":
    NlpTokenizer.tokenize('en', "I come from New York. I'd like to eat Pizza.", verbose=True)
    NlpTokenizer.tokenize('zh', "我来自纽约，想吃披萨。", verbose=True)
    NlpTokenizer.tokenize('ja', "私はニューヨークから来ました。ピザを食べたいのですが。", verbose=True)
    NlpTokenizer.tokenize('ko', "저는 뉴욕에서 왔습니다. 피자 먹고 싶어요.", verbose=True)
    NlpTokenizer.tokenize('hi', "मैं न्यूयॉर्क से आता हूं। मुझे पिज्जा खाना है।", verbose=True)
    NlpTokenizer.tokenize('vi', "Tôi đến từ New York. Tôi muốn ăn Pizza.", verbose=True)