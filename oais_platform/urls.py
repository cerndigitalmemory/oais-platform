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
router.register(r"users", views.UserViewSet)
router.register(r"groups", views.GroupViewSet)
router.register(r"archives", views.ArchiveViewSet)
router.register(r"steps", views.StepViewSet)
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
                ## User
                # Retrieve or set user information, including personal settings
                path("user/me/", views.user_get_set, name="me"),
                # Retrieve or add archives in the user staging area
                path(
                    "user/me/staging-area/",
                    views.ArchiveViewSet.as_view({
                        "get": "get_staging_area",
                        "post": "add_to_staging_area"
                    }),
                    name="staging_area",
                ),
                # (Currently unused)
                # Creates a new Archive given a Source and Record ID and triggers
                #  the Harvest step on it
                path(
                    "archive/create/harvest/",
                    views.create_by_harvest,
                    name="create-by-harvest",
                ),
                path(
                    "archive/create/<str:recid>/<str:source>/",
                    views.create_archive,
                    name="create_archive",
                ),
                path("archive/<int:id>/", views.get_steps, name="get_steps"),
                path(
                    "archive/<int:id>/details/",
                    views.archive_details,
                    name="archive_details",
                ),
                path(
                    "archive/next-step",
                    views.create_next_step,
                    name="next_step",
                ),
                path(
                    "archive/<int:id>/save-manifest/",
                    views.save_manifest,
                    name="save_manifest",
                ),
                path(
                    "archive/<int:pk>/get-collections/",
                    views.ArchiveViewSet.as_view({"get": "archive_collections"}),
                    name="get_collections",
                ),
                path(
                    "archive/<int:pk>/unstage/",
                    views.ArchiveViewSet.as_view({"get": "archive_unstage"}),
                    name="archive_unstage",
                ),
                path(
                    "archive/<int:pk>/delete/",
                    views.ArchiveViewSet.as_view({"get": "archive_delete"}),
                    name="archive_delete",
                ),
                path(
                    "get-archive-information-labels/",
                    views.get_archive_information_labels,
                    name="get_archive_information_labels",
                ),
                path(
                    "archives/unstage",
                    views.unstage_archives,
                    name="unstage_archives",
                ),
                path(
                    # Returns a list of similar archives (with the same recid + source)
                    "archive/<int:pk>/search/",
                    views.ArchiveViewSet.as_view({"get": "archive_search"}),
                    name="search",
                ),
                path(
                    # Gives a list of archives and returns for each archive a list with all the collections and the duplicates this archive has
                    "archives/detailed",
                    views.get_detailed_archives,
                    name="get_detailed_archives",
                ),
                path("user/me/tags", views.get_tags, name="get_tags"),
                path(
                    "records/check",
                    views.check_archived_records,
                    name="check_archived_records",
                ),
                path(
                    "steps/status",
                    views.get_steps_status,
                    name="get_steps_status",
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
            ]
        ),
    ),
]
