from fastapi import APIRouter

from r2base.http.schemas.heartbeat import Heartbeat

router = APIRouter()


@router.get("/ping", response_model=Heartbeat, name="ping")
def ping() -> Heartbeat:
    heartbeat = Heartbeat(is_alive=True)
    return heartbeat
