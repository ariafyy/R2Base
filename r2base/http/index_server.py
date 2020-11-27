from fastapi import FastAPI
from r2base.http.routes.router import index_router
from r2base.config import EnvVar
from r2base.http.event_handlers import (start_app_handler, stop_app_handler, exception_handler)
from r2base import ServerType

def get_app() -> FastAPI:
    fast_app = FastAPI(title=EnvVar.APP_NAME, version=EnvVar.APP_VERSION, debug=EnvVar.IS_DEBUG)
    fast_app.include_router(index_router, prefix=EnvVar.API_PREFIX)

    fast_app.add_event_handler("startup", start_app_handler(fast_app, ServerType.indexer))
    fast_app.add_event_handler("shutdown", stop_app_handler(fast_app, ServerType.indexer))
    fast_app.add_exception_handler(Exception, exception_handler)

    return fast_app


app = get_app()

