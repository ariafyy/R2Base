from fastapi import APIRouter
from starlette.requests import Request
from r2base.http.schemas.payload import RankReadBody
from r2base.http.schemas.response import SearchResult
from r2base.engine.ranker import Ranker
import time


router = APIRouter()
@router.post("/query", response_model=SearchResult, name="predict")
async def post_predict(
    request: Request,
    req: RankReadBody = None
) -> SearchResult:
    s_time = time.time()

    ranker: Ranker = request.app.state.model
    res = ranker.query(req.index, req.query)
    resp = SearchResult(took=time.time() - s_time, ranks=res, reads=[])
    return resp
