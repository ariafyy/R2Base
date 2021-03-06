from fastapi import APIRouter, status
from starlette.requests import Request
from r2base.http.schemas.payload import WriteDocBody, WriteIndexBody
from r2base.http.schemas.response import DocWrite, DocRead, IndexWrite, IndexRead, MappingRead, IndexList
from r2base.engine.indexer import Indexer
import time

router = APIRouter()


@router.get("/list", response_model=IndexList, name="Get Index List")
async def list_index(request: Request) -> IndexList:
    indexer: Indexer = request.app.state.indexer
    return IndexList(indexes=indexer.list())


@router.post("/{index_id}", response_model=IndexWrite, name="Create Index", status_code=status.HTTP_201_CREATED)
async def make_index(
        request: Request,
        index_id: str,
        body: WriteIndexBody = None
) -> IndexWrite:
    indexer: Indexer = request.app.state.indexer
    indexer.create_index(index_id, body.mappings)
    return IndexWrite(index=index_id, action='created')


@router.delete("/{index_id}", response_model=IndexWrite, name="Delete index")
async def delete_index(
        request: Request,
        index_id: str
) -> IndexWrite:
    indexer: Indexer = request.app.state.indexer
    indexer.delete_index(index_id)
    resp = IndexWrite(index=index_id, action='deleted')
    return resp


@router.get("/{index_id}", response_model=IndexRead, name="Get Index Info")
async def get_index_info(
        request: Request,
        index_id: str
) -> IndexRead:
    indexer: Indexer = request.app.state.indexer
    return IndexRead(index=index_id, size=indexer.size(index_id))


@router.get("/{index_id}/mappings", response_model=MappingRead, name="Get Mapping")
async def get_mapping(
        request: Request,
        index_id: str
) -> MappingRead:
    indexer: Indexer = request.app.state.indexer
    return MappingRead(index=index_id, mappings=indexer.get_mapping(index_id))


@router.post("/{index_id}/docs", response_model=DocWrite, name="Add documents", status_code=status.HTTP_201_CREATED)
async def add_docs(
        request: Request,
        index_id: str,
        block_data: WriteDocBody = None
) -> DocWrite:
    s_time = time.time()
    indexer: Indexer = request.app.state.indexer
    doc_ids = indexer.add_docs(index_id, block_data.docs, block_data.batch_size)
    return DocWrite(took=time.time() - s_time, doc_ids=doc_ids, action='added')


@router.put("/{index_id}/docs", response_model=DocWrite, name="Update documents")
async def update_docs(
        request: Request,
        index_id: str,
        body: WriteDocBody = None
) -> DocWrite:
    s_time = time.time()
    indexer: Indexer = request.app.state.indexer
    doc_ids = indexer.update_docs(index_id, body.docs, body.batch_size)
    return DocWrite(took=time.time() - s_time, doc_ids=doc_ids, action='updated')


@router.delete("/{index_id}/docs/{doc_ids}", response_model=DocWrite, name="Delete documents")
async def delete_docs(
        request: Request,
        index_id: str,
        doc_ids: str,
) -> DocWrite:
    s_time = time.time()
    indexer: Indexer = request.app.state.indexer
    if ',' in doc_ids:
        doc_ids = doc_ids.split(',')
    else:
        doc_ids = [doc_ids]
    res = indexer.delete_docs(index_id, doc_ids)
    resp = DocWrite(took=time.time() - s_time, doc_ids=doc_ids, action='deleted', msg=res)
    return resp


@router.get("/{index_id}/docs/{doc_ids}", response_model=DocRead, name="Read documents")
async def read_docs(
        request: Request,
        index_id: str,
        doc_ids: str,
) -> DocRead:
    if ',' in doc_ids:
        doc_ids = doc_ids.split(',')
    else:
        doc_ids = [doc_ids]

    indexer: Indexer = request.app.state.indexer
    docs = indexer.read_docs(index_id, doc_ids)
    resp = DocRead(docs=docs)
    return resp



