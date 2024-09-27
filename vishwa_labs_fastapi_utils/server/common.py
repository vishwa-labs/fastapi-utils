import os

from fastapi import FastAPI
from starlette.middleware.gzip import GZipMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics

from vishwa_labs_fastapi_utils.core import common
from vishwa_labs_fastapi_utils.server.route_handlers import TelemetryAPI


def instrument_server(service_name: str, app: FastAPI):
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.add_middleware(PrometheusMiddleware, app_name=service_name, group_paths=True,
                       labels={
                           "namespace": common.NAMESPACE,
                           "pod": common.POD_NAME,
                           "container": common.CONTAINER_NAME,
                           "node": os.getenv("KUBERNETES_NODE_NAME", "unknown_node")
                       }
                       )
    app.add_route("/metrics", handle_metrics)

    app.include_router(TelemetryAPI().router)
    return app
