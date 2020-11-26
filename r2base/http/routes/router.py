from fastapi import APIRouter

from r2base.http.routes import heartbeat, indexing, ranking

rank_router = APIRouter()
rank_router.include_router(heartbeat.router, tags=["health"], prefix="/v1/health")
rank_router.include_router(ranking.router, tags=["search"], prefix="/v1/search")


index_router = APIRouter()
index_router.include_router(heartbeat.router, tags=["health"], prefix="/v1/health")
index_router.include_router(indexing.router, tags=["index"], prefix="/v1/index")
