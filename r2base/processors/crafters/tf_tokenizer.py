from r2base.processors.bases import ProcessorBase
from soco_encoders.model_loaders import EncoderLoader


class TransformerTokenizer(ProcessorBase):
    models = {}

    def _get_model(self, model_id):
        if model_id not in self.models:
            self.models[model_id] = EncoderLoader.load_tokenizer(model_id)
        return self.models[model_id]

    def run(self, data, model_id=None):
        tokenizer = self._get_model(model_id)
        tokens = tokenizer.tokenize(data)
        return tokens