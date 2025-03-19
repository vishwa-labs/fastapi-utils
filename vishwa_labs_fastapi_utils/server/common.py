import os
from fastapi import FastAPI
from starlette.middleware.gzip import GZipMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics
from prometheus_client import CONTENT_TYPE_LATEST

from vishwa_labs_fastapi_utils.core import common
from vishwa_labs_fastapi_utils.server.route_handlers import TelemetryAPI
from vishwa_labs_fastapi_utils.metrics_handler import MetricBuilder

prom_metrics_manager = None

def instrument_server(
    service_name: str, 
    app: FastAPI, 
    custom_metric_builder: MetricBuilder = None, 
) -> FastAPI:
    global prom_metrics_manager

    # Add existing middleware and /metrics endpoint (for starlette_exporter)
    app.add_middleware(GZipMiddleware, minimum_size=1000)
    app.add_middleware(
        PrometheusMiddleware,
        app_name=service_name,
        group_paths=True,
        labels={
            "namespace": common.NAMESPACE,
            "pod": common.POD_NAME,
            "container": common.CONTAINER_NAME,
            "node": os.getenv("KUBERNETES_NODE_NAME", "unknown_node")
        }
    )
    app.add_route("/metrics", handle_metrics)

    # Initialize the global MetricBuilder
    if custom_metric_builder is None:
        prom_metrics_manager = MetricBuilder(service_name)
    else:
        prom_metrics_manager = custom_metric_builder

    # Include any additional routes (e.g., telemetry)
    app.include_router(TelemetryAPI().router)
    return app
