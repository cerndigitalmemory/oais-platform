"""oais_platform URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.1/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import include, path
from rest_framework import routers

from oais_platform.oais import views

router = routers.DefaultRouter()
router.register(r"users", views.UserViewSet)
router.register(r"groups", views.GroupViewSet)
router.register(r"archives", views.ArchiveViewSet)
router.register(r"steps", views.StepViewSet)
router.register(r"collections", views.CollectionViewSet)

from drf_spectacular.views import (
    SpectacularAPIView,
    SpectacularRedocView,
    SpectacularSwaggerView,
)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path("admin/", admin.site.urls),
    path(
        # Set base path
        r"api/",
        include(
            [
                path("schema/", SpectacularAPIView.as_view(), name="schema"),
                path(
                    "schema/swagger-ui/",
                    SpectacularSwaggerView.as_view(url_name="schema"),
                    name="swagger-ui",
                ),
                path(
                    "schema/redoc/",
                    SpectacularRedocView.as_view(url_name="schema"),
                    name="redoc",
                ),
                # Auth endpoint used by the browsable API of DRF
                path(
                    "api-auth/",
                    include("rest_framework.urls", namespace="rest_framework"),
                ),
                # Auth endpoint used for CERN SSO using OpenID Connect
                path("oidc/", include("mozilla_django_oidc.urls")),
                # Auth endpoints used to login/logout (local accounts)
                path("login/", views.login, name="login"),
                path("logout/", views.logout, name="logout"),
                # API
                path("", include(router.urls)),
                path("me/", views.me, name="me"),
                path(
                    "harvest/<str:recid>/<str:source>/", views.harvest, name="harvest"
                ),
                path(
                    "create-archive/<str:recid>/<str:source>/",
                    views.create_archive,
                    name="create_archive",
                ),
                path("upload/", views.upload, name="upload"),
                path("search/<str:source>/", views.search, name="search"),
                path(
                    "search/<str:source>/<str:recid>/",
                    views.search_by_id,
                    name="search_by_id",
                ),
                path("search-query/", views.search_query, name="search_query"),
                path("archive/<int:id>/", views.get_steps, name="get-steps"),
                path(
                    "archive-details/<int:id>",
                    views.archive_details,
                    name="archive_details",
                ),
                path(
                    "archive/next-step",
                    views.create_next_step,
                    name="next-step",
                ),
                path(
                    "save-manifest/<int:id>",
                    views.save_manifest,
                    name="save_manifest",
                ),
                path("settings/", views.get_settings, name="get_settings"),
                path(
                    "collection/<int:id>",
                    views.collection_details,
                    name="collection_details",
                ),
                path(
                    "record-check/",
                    views.check_archived_records,
                    name="check_archived_records",
                ),
                path(
                    "create-collection/",
                    views.create_collection,
                    name="create_collection",
                ),
                path(
                    "collections/<int:pk>/actions/delete/",
                    views.CollectionViewSet.as_view({"post": "delete"}),
                    name="collections-delete",
                ),
                path(
                    "collections/<int:pk>/actions/add/",
                    views.CollectionViewSet.as_view({"post": "add"}),
                    name="add-to-collection",
                ),
                path(
                    "collections/<int:pk>/actions/remove/",
                    views.CollectionViewSet.as_view({"post": "remove"}),
                    name="remove-from-collection",
                ),
                path(
                    "collections/",
                    views.CollectionViewSet.as_view({"get": "get_queryset"}),
                    name="get-collections",
                ),
                path(
                    "archive/<int:pk>/get-collections/",
                    views.ArchiveViewSet.as_view({"get": "archive_collections"}),
                    name="get-collections",
                ),
                path(
                    # Returns a list of similar archives (with the same recid + source)
                    "archive/<int:pk>/search/",
                    views.ArchiveViewSet.as_view({"get": "archive_search"}),
                    name="search",
                ),
                path(
                    # Returns all the archives that are in the staged phase (not in a collection, no step intitiallized)
                    "users/<int:pk>/archives-staged/",
                    views.UserViewSet.as_view({"get": "archives_staged"}),
                    name="archives-staged",
                ),
                path(
                    # Gives a list of archives and returns for each archive a list with all the collections and the duplicates this archive has
                    "get-detailed/",
                    views.get_detailed_archives,
                    name="get_detailed_archives",
                ),
            ]
        ),
    ),
]

# Uncomment the following lines to serve the contents of the "static" folder in
#  the root of the repository as static.
# (This can be used during development to serve a build of `oais-web`)

from django.conf.urls.static import static

urlpatterns += static("/", document_root="public")
