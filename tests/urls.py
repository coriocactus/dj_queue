from django.contrib import admin
from django.urls import include, path


urlpatterns = [
  path("admin/", admin.site.urls),
  path("dj-queue/", include("dj_queue.urls")),
]
