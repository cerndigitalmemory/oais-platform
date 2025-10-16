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
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

from oais_platform.oais import views

# Register class-based views
router = routers.DefaultRouter()
router.register(r"users", views.UserViewSet, basename="users")
router.register(r"archives", views.ArchiveViewSet, basename="archives")
router.register(r"steps", views.StepViewSet, basename="steps")
router.register(r"tags", views.TagViewSet, basename="tags")
router.register(r"step-type", views.StepTypeViewSet, basename="step-type")


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
                ## Main API surface
                path("", include(router.urls)),
                # Upload a file
                path("upload/file", views.upload_file, name="upload-file"),
                # Upload a SIP
                path("upload/sip", views.upload_sip, name="upload-sip"),
                # Parse full URL of a supported source to find the record ID
                path(
                    "search/parse-url/",
                    views.parse_url,
                    name="parse_url",
                ),
                # Search
                path("search/<str:source>/", views.search, name="search"),
                path(
                    "search/<str:source>/<str:recid>/",
                    views.search_by_id,
                    name="search_by_id",
                ),
                # Upload a SIP by announcing its location (e.g. EOS)
                path(
                    "upload/announce/",
                    views.announce,
                    name="announce",
                ),
                # Upload a batch of SIPs by announcing its location (e.g. EOS)
                path(
                    "upload/batch-announce/",
                    views.batch_announce,
                    name="batch-announce",
                ),
                path("stats/", views.statistics, name="statistics"),
                path("stats/steps", views.step_statistics, name="step_statistics"),
                path("sources/", views.sources, name="sources"),
                path("token/", TokenObtainPairView.as_view(), name="token_obtain_pair"),
                path(
                    "token/refresh/", TokenRefreshView.as_view(), name="token_refresh"
                ),
            ]
        ),
    ),
]
