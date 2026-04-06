from django.apps import apps


def test_app_installed():
  assert apps.is_installed("dj_queue")
