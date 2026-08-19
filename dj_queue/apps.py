from django.apps import AppConfig, apps


class DjQueueConfig(AppConfig):
  name = "dj_queue"
  verbose_name = "dj_queue"

  def ready(self):
    if not apps.is_installed("django.contrib.admin"):
      return

    from django.contrib import admin

    from dj_queue.admin import _install_dj_queue_admin_site

    _install_dj_queue_admin_site(admin.site)
