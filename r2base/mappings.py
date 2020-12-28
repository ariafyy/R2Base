from pydantic import BaseModel


class KeywordMapping(BaseModel):
    type: str


class VectorMapping(BaseModel):
    type: str
    num_dim: int


class TextMapping(BaseModel):
    lang: str
    index: str
    processor: str = None
    model_id: str = None
    q_processor: str = None
    q_model_id: str = None

    def __init__(self, **data):
        # initialize default values here for different index types
        if 'q_processor' not in data:
            data['q_processor'] = data.get('processor')

        if 'q_model_id' not in data:
            data['q_model_id'] = data.get('processor')

        super().__init__(**data)



if __name__ == "__main__":
    x = TextMapping.parse_obj({"type": "keyword", "lang": "zh", "index": "bm25"})
    print(x['type'])
    print(x)
