from fastapi import APIRouter
from starlette.requests import Request
from r2base.http.schemas.payload import AddDocPayload, CreateIndexPayload
from r2base.http.schemas.response import DocWriteResult, IndexWriteResult
from r2base.engine.indexer import Indexer
import time

router = APIRouter()


@router.post("/add_doc", response_model=DocWriteResult, name="Add documents")
async def post_predict(
    request: Request,
    block_data: AddDocPayload = None
) -> DocWriteResult:
    s_time = time.time()
    indexer: Indexer = request.app.state.model
    doc_ids = indexer.add_docs(block_data.index, block_data.docs, block_data.batch_size)
    resp = DocWriteResult(took=time.time() - s_time, doc_ids=doc_ids)
    return resp


@router.post("/create_index", response_model=IndexWriteResult, name="Create Index")
async def post_predict(
    request: Request,
    block_data: CreateIndexPayload = None
) -> IndexWriteResult:
    s_time = time.time()
    indexer: Indexer = request.app.state.model
    indexer.create_index(block_data.index, block_data.mapping)
    resp = IndexWriteResult(took=time.time() - s_time, index=block_data.index)
    return resp