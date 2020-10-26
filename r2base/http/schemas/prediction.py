from pydantic import BaseModel


class LegalDocPredictionResult(BaseModel):
    took: int
    result: list = []
