from fastapi import APIRouter
from starlette.requests import Request
from r2base.http.schemas.payload import SearchBody
from r2base.http.schemas.response import Search, ScrollSearch
from r2base.engine.ranker import Ranker
import time

router = APIRouter()


@router.post("/{index_id}/query", response_model=Search, name="predict")
async def post_predict(
        request: Request,
        index_id: str,
        body: SearchBody = None
) -> Search:
    s_time = time.time()
    ranker: Ranker = request.app.state.ranker
    res = ranker.query(index_id, body.query)
    resp = Search(took=time.time() - s_time, ranks=res, reads=[])
    return resp


@router.post("/{index_id}/scroll", response_model=ScrollSearch, name="predict")
async def post_scroll(
        request: Request,
        index_id: str,
        body: SearchBody = None
) -> Search:
    s_time = time.time()
    ranker: Ranker = request.app.state.ranker
    res, last_id = ranker.scroll_query(index_id, body.query)
    resp = ScrollSearch(took=time.time() - s_time, ranks=res, reads=[], last_id=last_id)
    return resp
