from fastapi import FastAPI
from starlette.middleware.gzip import GZipMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics

from xpuls_fastapi_utils.server.routers import TelemetryAPI


def instrument_server(service_name: str, app: FastAPI):
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.add_middleware(PrometheusMiddleware, app_name=service_name, group_paths=True)
    app.add_route("/metrics", handle_metrics)

    app.include_router(TelemetryAPI().router)
    return app
