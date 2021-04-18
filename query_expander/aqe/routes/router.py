from fastapi import APIRouter

from aqe.routes import heartbeat, predict

router = APIRouter()
router.include_router(heartbeat.router, tags=["health"], prefix="/v1/health")
router.include_router(predict.router, tags=["predict"], prefix="/v1/predict")


