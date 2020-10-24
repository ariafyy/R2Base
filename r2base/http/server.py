from fastapi import FastAPI
from r2base.routes.router import api_router
from r2base.config import EnvVars
from r2base.event_handlers import (start_app_handler, stop_app_handler)


def get_app() -> FastAPI:
    fast_app = FastAPI(title=EnvVars.APP_NAME, version=EnvVars.APP_VERSION, debug=EnvVars.IS_DEBUG)
    fast_app.include_router(api_router, prefix=EnvVars.API_PREFIX)

    fast_app.add_event_handler("startup", start_app_handler(fast_app))
    fast_app.add_event_handler("shutdown", stop_app_handler(fast_app))

    return fast_app


app = get_app()