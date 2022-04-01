from django.urls import path

from oais_platform.search.views import SearchArchives

urlpatterns = [
    path("archives/", SearchArchives.as_view()),
]
