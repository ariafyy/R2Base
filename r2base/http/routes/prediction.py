from fastapi import APIRouter
from starlette.requests import Request
from legal_parser.models.payload import LegalDocPayload
from legal_parser.models.prediction import LegalDocPredictionResult
from legal_parser.services.parser import LegalDocParser

router = APIRouter()


@router.post("/predict", response_model=LegalDocPredictionResult, name="predict")
async def post_predict(
    request: Request,
    block_data: LegalDocPayload = None
) -> LegalDocPredictionResult:

    model: LegalDocParser = request.app.state.model
    prediction: LegalDocPredictionResult = model.parse(block_data)

    return prediction
