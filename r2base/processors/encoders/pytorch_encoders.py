from r2base.processors.bases import ProcessorBase
from soco_encoders.model_loaders import EncoderLoader


class TransformerEncoders(ProcessorBase):
    model_ids = {}

    def _get_model(self, model_id):
        if model_id not in self.model_ids:
            self.model_ids[model_id] = EncoderLoader.load_model('pretrain-models',
                                                                model_id, use_gpu=True)

        return self.model_ids[model_id]

    def run(self, data, model_id=None):
        vector = self._get_model(model_id).encode([data])
        return vector