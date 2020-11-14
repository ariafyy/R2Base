from fastapi import APIRouter

from r2base.http.routes import heartbeat, indexing, ranking

api_router = APIRouter()
api_router.include_router(heartbeat.router, tags=["health"], prefix="/v1/health")
api_router.include_router(indexing.router, tags=["indexing"], prefix="/v1/index")
api_router.include_router(ranking.router, tags=["ranking"], prefix="/v1/search")
