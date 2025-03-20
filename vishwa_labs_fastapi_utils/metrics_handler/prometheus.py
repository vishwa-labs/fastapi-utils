import os
from enum import Enum

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Gauge,
    Counter,
    push_to_gateway,
    generate_latest,
    multiprocess,
)


class MetricType(Enum):
    GAUGE = "gauge"
    COUNTER = "counter"


class MetricBuilder:
    def __init__(self, job_name, push_gateway_url=None):
        """
        Initialize MetricBuilder.

        Args:
            job_name (str): Job name for metrics pushed to Prometheus.
            push_gateway_url (str, optional): URL of the Prometheus Push Gateway.
                If not provided, it falls back to the environment variable PUSHGATEWAY_URL or a default.
            use_default_registry (bool, optional): Whether to use the default Prometheus registry.
                If False, CollectRegistry() is used.
        """
        self.registry = REGISTRY
        if (
            "prometheus_multiproc_dir" in os.environ
            or "PROMETHEUS_MULTIPROC_DIR" in os.environ
        ):
            self.registry = CollectorRegistry()
            multiprocess.MultiProcessCollector(self.registry)

        self.metrics = {}
        self.push_gateway_url = push_gateway_url or os.getenv(
            "PUSHGATEWAY_URL", "http://localhost:9091"
        )
        self.job_name = job_name

    def create_metric(
        self, name, description, label_names=None, metric_type=MetricType.GAUGE
    ):
        """
        Create a Prometheus Gauge metric with optional labels.

        Args:
            name (str): Name of the metric.
            description (str): Description of the metric.
            label_names (list, optional): List of label names for the metric.
            metric_type (MetricType, optional): Type of the metric (Gauge or Counter).
        """
        if name not in self.metrics:
            if metric_type == MetricType.COUNTER:
                self.metrics[name] = Counter(
                    name,
                    description,
                    labelnames=label_names or [],
                    registry=self.registry,
                )
            elif metric_type == MetricType.GAUGE:
                self.metrics[name] = Gauge(
                    name,
                    description,
                    labelnames=label_names or [],
                    registry=self.registry,
                )
            else:
                raise ValueError(f"Unsupported metric type: {metric_type}")

    def update_metric(self, name, value, labels=None, increment=False):
        """
        Update the value of a Prometheus metric with optional labels.

        Args:
            name (str): Name of the metric.
            value (float): Value to set for the metric.
            labels (dict, optional): Dictionary of labels and their values.
            increment (bool, optional): Whether to increment the metric instead of setting it.
        """
        if name in self.metrics:
            metric = self.metrics[name]
            if labels:
                metric = metric.labels(**labels)
            if increment and hasattr(metric, "inc"):
                metric.inc(value)
            else:
                metric.set(value)
        else:
            raise KeyError(f"Metric '{name}' not found. Please create it first.")

    def create_or_update_metric(
        self,
        name,
        description,
        value,
        labels=None,
        increment=False,
        metric_type=MetricType.GAUGE,
    ):
        """
        Create a new metric or update an existing one.

        Args:
            name (str): Name of the metric.
            description (str): Description of the metric.
            value (float): Value to set for the metric.
            labels (dict, optional): Dictionary of labels and their values.
            increment (bool, optional): Whether to increment the metric instead of setting it.
            metric_type (MetricType, optional): Type of the metric (Gauge or Counter).
        """
        if name not in self.metrics:
            self.create_metric(
                name,
                description,
                label_names=labels.keys() if labels else [],
                metric_type=metric_type,
            )
        self.update_metric(name, value, labels, increment)

    def push_metrics(self):
        """
        Push all metrics to the Prometheus Push Gateway.
        """
        push_to_gateway(
            self.push_gateway_url, job=self.job_name, registry=self.registry
        )

    def get_metrics(self):
        """
        Retrieve all current metrics in Prometheus exposition format.

        Returns:
            bytes: The latest metrics as a bytestring.
        """
        return generate_latest(self.registry)
