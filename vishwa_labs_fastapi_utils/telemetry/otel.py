from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry import trace
from opentelemetry.sdk.resources import SERVICE_NAME, Resource


def get_kubernetes_namespace():
    try:
        with open(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace", "r"
        ) as file:
            return file.read().strip()
    except FileNotFoundError:
        return "default"


def init(service_name: str, service_type: str = "svc", tags: dict = {}):
    resource = Resource(
        attributes=dict(
            {
                SERVICE_NAME: service_name,
                "service_type": service_type,
                "k8s.namespace": get_kubernetes_namespace(),
            },
            **tags
        )
    )

    tracer_provider = TracerProvider(resource=resource)

    otlp_exporter = OTLPSpanExporter(
        endpoint="http://tempo-distributor.monitoring:4317",
        insecure=True,
    )

    span_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(span_processor)

    trace.set_tracer_provider(tracer_provider)


def trace_step(step_name):
    def decorator(func):
        def wrapper(*args, **kwargs):
            with tracer.start_as_current_span(step_name):
                result = func(*args, **kwargs)
            return result

        return wrapper

    return decorator


tracer = trace.get_tracer(__name__)
