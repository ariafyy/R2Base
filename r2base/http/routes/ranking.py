from fastapi import APIRouter
from starlette.requests import Request
from r2base.http.schemas.payload import SearchBody
from r2base.http.schemas.response import Search, DocRead, MappingRead, IndexRead
from r2base.engine.ranker import Ranker
import time

router = APIRouter()


@router.get("/{index_id}", response_model=IndexRead, name="Get Index Info")
async def post_predict(
        request: Request,
        index_id: str
) -> IndexRead:
    ranker: Ranker = request.app.state.model
    return IndexRead(index=index_id, size=ranker.size(index_id))


@router.get("/{index_id}/mappings", response_model=MappingRead, name="Get Mapping")
async def post_predict(
        request: Request,
        index_id: str
) -> MappingRead:
    ranker: Ranker = request.app.state.model
    resp = MappingRead(mappings=ranker.get_mapping(index_id))
    return resp


@router.get("/{index_id}/docs/{doc_ids}", response_model=DocRead, name="Read documents")
async def post_predict(
        request: Request,
        index_id: str,
        doc_ids: str,
) -> DocRead:
    if ',' in doc_ids:
        doc_ids = doc_ids.split(',')

    ranker: Ranker = request.app.state.model
    docs = ranker.read_docs(index_id, doc_ids)
    resp = DocRead(docs=docs)
    return resp


@router.post("/{index_id}/query", response_model=Search, name="predict")
async def post_predict(
        request: Request,
        index_id: str,
        body: SearchBody = None
) -> Search:
    s_time = time.time()
    ranker: Ranker = request.app.state.model
    res = ranker.query(index_id, body.query)
    resp = Search(took=time.time() - s_time, ranks=res, reads=[])
    return resp
