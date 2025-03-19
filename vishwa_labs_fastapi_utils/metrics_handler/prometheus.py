import os

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway


class MetricBuilder:
    def __init__(self, job_name):
        """
        Initialize MetricBuilder.

        Args:
            push_gateway_url (str): URL of the Prometheus Push Gateway.
            job_name (str): Job name for metrics pushed to Prometheus.
        """
        self.registry = CollectorRegistry()
        self.metrics = {}
        self.push_gateway_url = os.getenv("PUSHGATEWAY_URL", "http://localhost:9091")
        self.job_name = job_name

    def create_metric(self, name, description, label_names=None):
        """
        Create a Prometheus Gauge metric with optional labels.

        Args:
            name (str): Name of the metric.
            description (str): Description of the metric.
            label_names (list): List of label names for the metric.
        """
        if name not in self.metrics:
            self.metrics[name] = Gauge(name, description, labelnames=label_names or [], registry=self.registry)

    def update_metric(self, name, value, labels=None):
        """
        Update the value of a Prometheus metric with optional labels.

        Args:
            name (str): Name of the metric.
            value (float): Value to set for the metric.
            labels (dict): Dictionary of labels and their values.
        """
        if name in self.metrics:
            if labels:
                self.metrics[name].labels(**labels).set(value)
            else:
                self.metrics[name].set(value)
        else:
            raise KeyError(f"Metric '{name}' not found. Please create it first.")

    def create_or_update_metric(self, name, description, value, labels=None):
        if name in self.metrics:
            self.update_metric(name, value, labels)
        else:
            self.create_metric(name, description, label_names=labels.keys() if labels is not None else [])
            self.update_metric(name, value, labels)



    def push_metrics(self):
        """
        Push all metrics to the Prometheus Push Gateway.
        """
        push_to_gateway(self.push_gateway_url, job=self.job_name, registry=self.registry)

