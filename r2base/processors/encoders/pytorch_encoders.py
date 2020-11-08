from r2base.processors.bases import ProcessorBase
from soco_encoders.model_loaders import EncoderLoader


class TransformerEncoders(ProcessorBase):
    model_ids = {}

    def _get_model(self, model_id):
        if model_id not in self.model_ids:
            self.model_ids[model_id] = EncoderLoader.load_model('pretrain-models',
                                                                model_id, use_gpu=False)

        return self.model_ids[model_id]

    def run(self, data, model_id=None, mode='default'):
        if mode == 'tscore':
            args = {'start_symbol': '<a>', 'end_symbol': '</a>', 'return_meta': True,
                    'min_threshold': 1e-2,
                    'top_k': 768,
                    'to_sparse': 'csr',
                    'term_batch_size': 3000,
                    'force_seq_len': -1,
                    'approx_padding': False,
                    'mask_padding': True
                    }
        else:
            args = None

        vector = self._get_model(model_id).encode([data], mode=mode, args=args)
        if type(vector) is tuple:
            f, meta = vector
            result = []
            for row_id in range(f.shape[0]):
                coo_row = f[row_id].tocoo()
                terms = []
                for col_id, v in zip(coo_row.col, coo_row.data):
                    terms.append((meta['vocab'][col_id], float(v),))
                terms = sorted(terms, key=lambda x: x[1] * -1)
                result.append(terms)
        else:
            result = vector
        return result

