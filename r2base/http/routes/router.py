from fastapi import APIRouter

from r2base.http.routes import heartbeat, indexing, ranking

router = APIRouter()
router.include_router(heartbeat.router, tags=["health"], prefix="/v1/health")
router.include_router(ranking.router, tags=["search"], prefix="/v1/search")
router.include_router(indexing.router, tags=["index"], prefix="/v1/index")


