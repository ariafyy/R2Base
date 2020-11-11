from r2base.processors.bases import ProcessorBase
from soco_encoders.model_loaders import EncoderLoader
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
        if type(data) is list:
            return [tokenizer.tokenize(l) for l in data]
        else:
            return tokenizer.tokenize(data)
