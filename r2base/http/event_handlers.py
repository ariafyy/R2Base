from typing import Callable
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from r2base.engine.indexer import Indexer
from r2base.engine.ranker import Ranker
from r2base import ServerType as ST
import logging
logger = logging.getLogger(__name__)


def _startup_indexer(app: FastAPI) -> None:
    model_instance = Indexer()
    app.state.model = model_instance


def _startup_ranker(app: FastAPI) -> None:
    model_instance = Ranker()
    app.state.model = model_instance


def _shutdown_model(app: FastAPI) -> None:
    app.state.model = None


def start_app_handler(app: FastAPI, mode: str) -> Callable:
    def startup() -> None:
        logger.info("Running app start handler.")
        if mode == ST.indexer:
            _startup_indexer(app)
        elif mode == ST.ranker:
            _startup_ranker(app)
    return startup


def stop_app_handler(app: FastAPI, mode: str) -> Callable:
    def shutdown() -> None:
        logger.info("Running app shutdown handler.")
        _shutdown_model(app)
    return shutdown


async def exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={'detail': str(exc)},
    )