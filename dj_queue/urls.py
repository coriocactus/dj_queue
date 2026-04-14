from django.urls import path

from dj_queue.contrib.prometheus import DjQueueCollector
from dj_queue.views import observability_metrics_view, observability_stats_view


app_name = "dj_queue"

urlpatterns = [
  path("stats.json", observability_stats_view, name="stats"),
  *(
    [path("metrics", observability_metrics_view, name="metrics")]
    if DjQueueCollector is not None
    else []
  ),
]
