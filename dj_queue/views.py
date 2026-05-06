from django.conf import settings
from django.core.exceptions import ImproperlyConfigured
from django.http import HttpResponse, JsonResponse
from django.utils.crypto import constant_time_compare

from dj_queue import observability
from dj_queue.contrib.prometheus import generate_latest, registry

PROMETHEUS_UNAVAILABLE = "install dj-queue[prometheus] to enable /dj_queue/metrics\n"


def observability_stats_view(request):
  auth_response = _observability_auth_response(request)
  if auth_response is not None:
    return auth_response
  return JsonResponse(observability.stats_payload())


def observability_metrics_view(request):
  auth_response = _observability_auth_response(request)
  if auth_response is not None:
    return auth_response
  if generate_latest is None or registry is None:
    return HttpResponse(PROMETHEUS_UNAVAILABLE, status=503, content_type="text/plain")
  output = generate_latest(registry)
  return HttpResponse(output, content_type="text/plain; version=0.0.4; charset=utf-8")


def _observability_auth_response(request):
  token = getattr(settings, "DJ_QUEUE_OBSERVABILITY_TOKEN", None)
  if token in (None, ""):
    return None
  if not isinstance(token, str):
    raise ImproperlyConfigured("DJ_QUEUE_OBSERVABILITY_TOKEN must be a string when set")

  scheme, _, credentials = request.headers.get("Authorization", "").partition(" ")
  if scheme.lower() == "bearer" and constant_time_compare(credentials, token):
    return None

  response = HttpResponse(status=401)
  response["WWW-Authenticate"] = "Bearer"
  return response
