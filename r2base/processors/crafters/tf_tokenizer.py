from r2base.processors.bases import ProcessorBase
from soco_tokenizer.model_loaders import EncoderLoader
from typing import Union, List


class TransformerTokenizer(ProcessorBase):
    models = {}

    def _get_model(self, model_id):
        if model_id not in self.models:
            self.models[model_id] = EncoderLoader.load_tokenizer(model_id)
        return self.models[model_id]

    def run(self, data: Union[List[str], str], **kwargs) -> Union[List[List[str]], List[str]]:
        model_id = kwargs['model_id']
        tokenizer = self._get_model(model_id)
        args = {'mode': kwargs.get('mode', 'char'),
                'normalize': kwargs.get('normalize', True),
                "max_len": kwargs.get('max_len', 1e10)
                }
        if type(data) is list:
            return [tokenizer.tokenize(l, **args) for l in data]
        else:
            return tokenizer.tokenize(data, **args)

