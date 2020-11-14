
from fastapi import APIRouter

from r2base.http.schemas.heartbeat import HeartbeatResult

router = APIRouter()


@router.get("/ping", response_model=HeartbeatResult, name="ping")
def get_hearbeat() -> HeartbeatResult:
    heartbeat = HeartbeatResult(is_alive=True)
    return heartbeat
