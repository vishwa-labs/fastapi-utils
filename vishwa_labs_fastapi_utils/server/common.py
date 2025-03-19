import os
from fastapi import FastAPI, Response, status
from starlette.middleware.gzip import GZipMiddleware
from starlette_exporter import PrometheusMiddleware, handle_metrics
from prometheus_client import CONTENT_TYPE_LATEST

from vishwa_labs_fastapi_utils.core import common
from vishwa_labs_fastapi_utils.server.route_handlers import TelemetryAPI
from vishwa_labs_fastapi_utils.metrics_handler import MetricBuilder

# Global variable to hold the MetricBuilder instance
global_metric_builder = None

def instrument_server(
    service_name: str, 
    app: FastAPI, 
    custom_metric_builder: MetricBuilder = None, 
    push_metrics: bool = False
) -> FastAPI:
    global global_metric_builder

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
        global_metric_builder = MetricBuilder(service_name)
    else:
        global_metric_builder = custom_metric_builder

    # Expose the custom metrics for pull mode via an endpoint
    @app.get("/custom-metrics")
    async def custom_metrics():
        return Response(
            content=global_metric_builder.get_metrics(), 
            media_type=CONTENT_TYPE_LATEST
        )

    # Optionally, add an endpoint to push metrics to the Push Gateway
    if push_metrics:
        @app.get("/push-metrics")
        async def push_metrics_route():
            try:
                global_metric_builder.push_metrics()
                return {"status": "success", "message": "Metrics pushed to push gateway"}
            except Exception as e:
                return Response(
                    content=str(e), 
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
                )

    # Include any additional routes (e.g., telemetry)
    app.include_router(TelemetryAPI().router)
    return app
