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
from django.urls import include, path

from rest_framework import routers
from rest_framework.authtoken import views as authtoken_views

from oais_platform.oais import views

router = routers.DefaultRouter()
router.register(r"users", views.UserViewSet)
router.register(r"groups", views.GroupViewSet)
router.register(r"records", views.RecordViewSet)
router.register(r"archives", views.ArchiveViewSet)

# Wire up our API using automatic URL routing.
# Additionally, we include login URLs for the browsable API.
urlpatterns = [
    path(
        # Set base path
        r"api/",
        include(
            [
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
                path("search/<str:source>/", views.search, name="search"),
                path(
                    "search/<str:source>/<str:recid>/",
                    views.search_by_id,
                    name="search_by_id",
                ),
            ]
        ),
    )
]

# Uncomment the following lines to serve the contents of the "static" folder in
#  the root of the repository as static.
# (This can be used during development to serve a build of `oais-web`)

# from django.conf.urls.static import static
# urlpatterns += static("/", document_root="static")
