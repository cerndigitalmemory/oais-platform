"""
OAIS Platform URL configuration

This file provides the mapping between the paths we expose in the API
and the views (or functions) handling them.

Endpoints retrieving paginated querysets should be handled by Class-based views.
"""

from django.contrib import admin
from django.urls import include, path
from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)
from rest_framework import routers

from oais_platform.oais import views

router = routers.DefaultRouter()
router.register(r"users", views.UserViewSet, basename="users")
router.register(r"groups", views.GroupViewSet, basename="groups")
router.register(r"archives", views.ArchiveViewSet, basename="archives")
router.register(r"steps", views.StepViewSet, basename="steps")
router.register(r"tags", views.TagViewSet, basename="tags")


# Wire up our API using automatic URL routing.
urlpatterns = [
    # Serve the Django Admin panel
    path("admin/", admin.site.urls),
    path(
        # Set base path
        r"api/",
        include(
            [
                ## Spectacular DRF (API documentation)
                # Serve the generated OpenAPI schema (as a YAML file)
                path("schema/", SpectacularAPIView.as_view(), name="schema"),
                # Serve the Swagger UI
                path(
                    "schema/swagger-ui/",
                    SpectacularSwaggerView.as_view(url_name="schema"),
                    name="swagger-ui",
                ),
                # Serve Redoc
                path(
                    "schema/redoc/",
                    SpectacularRedocView.as_view(url_name="schema"),
                    name="redoc",
                ),
                ## Authentication endpoints
                # "Login" used by the browsable API of DRF
                path(
                    "api-auth/",
                    include("rest_framework.urls", namespace="rest_framework"),
                ),
                # CERN SSO through OpenID Connect
                path("oidc/", include("mozilla_django_oidc.urls")),
                # Conventional login/logout ("local" accounts)
                path("login/", views.login, name="login"),
                path("logout/", views.logout, name="logout"),
                # API
                path("", include(router.urls)),
                path(
                    "users/me/staging-area/",
                    views.ArchiveViewSet.as_view(
                        {"get": "get_staging_area", "post": "add_to_staging_area"}
                    ),
                    name="staging_area",
                ),
                path(
                    "archives/create/<str:recid>/<str:source>/",
                    views.ArchiveViewSet.as_view({"post": "archive_create"}),
                    name="archives-create",
                ),
                path(
                    "get-archive-information-labels/",
                    views.get_archive_information_labels,
                    name="get_archive_information_labels",
                ),
                path(
                    "records/check",
                    views.check_archived_records,
                    name="check_archived_records",
                ),
                path("harvest/<int:id>/", views.harvest, name="harvest"),
                path("upload/", views.upload, name="upload"),
                path("search/<str:source>/", views.search, name="search"),
                path(
                    "search/<str:source>/<str:recid>/",
                    views.search_by_id,
                    name="search_by_id",
                ),
                path("search-query/", views.search_query, name="search_query"),
                path("settings/", views.get_settings, name="get_settings"),
                path(
                    "search/parse-url/",
                    views.parse_url,
                    name="parse_url",
                ),
                path(
                    "upload/announce/",
                    views.announce,
                    name="announce",
                ),
            ]
        ),
    ),
]
