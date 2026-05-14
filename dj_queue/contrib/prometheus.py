try:
  from prometheus_client import CollectorRegistry, generate_latest
  from prometheus_client.core import GaugeMetricFamily
except ImportError:
  DjQueueCollector = None
  registry = None
  generate_latest = None
else:
  from dj_queue.metrics import metric_families

  class DjQueueCollector:
    """Prometheus collector that exposes dj_queue metrics from the shared observability snapshot."""

    def collect(self):
      for family in metric_families():
        gauge = GaugeMetricFamily(
          family.name,
          family.help_text,
          labels=list(family.labels),
        )
        for sample in family.samples:
          gauge.add_metric(list(sample.labels), sample.value)
        yield gauge

  registry = CollectorRegistry(auto_describe=False)
  registry.register(DjQueueCollector())
