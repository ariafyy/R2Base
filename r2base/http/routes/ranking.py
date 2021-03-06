from fastapi import APIRouter
from starlette.requests import Request
from r2base.http.schemas.payload import SearchBody
from r2base.http.schemas.response import Search, ScrollSearch, DocQueryWrite
from r2base.engine.ranker import Ranker
import time

router = APIRouter()


@router.post("/{index_id}/query", response_model=Search, name="ranking query")
async def query(
        request: Request,
        index_id: str,
        body: SearchBody = None
) -> Search:
    s_time = time.time()
    ranker: Ranker = request.app.state.ranker
    res = ranker.query(index_id, body.query)
    resp = Search(took=time.time() - s_time, ranks=res, reads=[])
    return resp


@router.post("/{index_id}/scroll_query", response_model=ScrollSearch, name="scroll query")
async def scroll_query(
        request: Request,
        index_id: str,
        body: SearchBody = None
) -> ScrollSearch:
    s_time = time.time()
    ranker: Ranker = request.app.state.ranker
    res, last_id = ranker.scroll_query(index_id, body.query)
    resp = ScrollSearch(took=time.time() - s_time, docs=res, last_id=last_id)
    return resp


@router.post("/{index_id}/delete_query", response_model=DocQueryWrite, name="delete query")
async def delete_query(
        request: Request,
        index_id: str,
        body: SearchBody = None
) -> DocQueryWrite:
    s_time = time.time()
    ranker: Ranker = request.app.state.ranker
    res = ranker.delete_query(index_id, body.query)
    resp = DocQueryWrite(took=time.time() - s_time, action='deleted', msg=res)
    return resp



