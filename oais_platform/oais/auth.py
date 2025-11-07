import logging

import requests
from django.contrib.auth.models import User
from django.utils import timezone
from mozilla_django_oidc.auth import OIDCAuthenticationBackend
from rest_framework.authentication import BaseAuthentication
from rest_framework.exceptions import AuthenticationFailed

from oais_platform.settings import (
    AUTH_SERVICE_ENDPOINT,
    AUTH_SERVICE_TOKEN_ENDPOINT,
    OIDC_RP_CLIENT_ID,
    OIDC_RP_CLIENT_SECRET,
)

from .models import PersonalAccessToken


class CERNAuthenticationBackend(OIDCAuthenticationBackend):
    def filter_users_by_claims(self, claims):
        username = claims.get("cern_upn")
        if not username:
            return self.UserModel.objects.none()
        try:
            user = self.UserModel.objects.filter(username=username)
            return user
        except User.DoesNotExist:
            return self.UserModel.objects.none()

    def create_user(self, claims):
        username = claims["cern_upn"]
        email = claims["email"]
        user = self.UserModel.objects.create_user(username, email=email)
        return self.update_user(user, claims)

    def update_user(self, user, claims):
        user.first_name = claims["given_name"]
        user.last_name = claims["family_name"]
        user.profile.department = self.get_user_department(user)
        user.save()
        return user

    def get_user_department(self, user):
        api_token = self.get_auth_service_token()
        authzsvc_endpoint = AUTH_SERVICE_ENDPOINT
        identity = user.username

        identities = requests.get(
            "{0}Identity/{1}".format(authzsvc_endpoint, identity),
            headers={"Authorization": "Bearer {}".format(api_token)},
            verify=False,
        )
        try:
            return identities.json()["data"]["cernDepartment"]
        except Exception as e:
            logging.warning("Could not determine User's department.")
            logging.debug(str(e))
            return None

    def get_auth_service_token(self):
        auth_service_api_token_endpoint = AUTH_SERVICE_TOKEN_ENDPOINT

        token_resp = requests.post(
            auth_service_api_token_endpoint,
            data={
                "grant_type": "client_credentials",
                "client_id": OIDC_RP_CLIENT_ID,
                "client_secret": OIDC_RP_CLIENT_SECRET,
                "audience": "authorization-service-api",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        try:
            return token_resp.json()["access_token"]
        except Exception as e:
            logging.error("Could not obtain authorization service api token.")
            logging.debug(str(e))
            return None


class PersonalAccessTokenAuthentication(BaseAuthentication):
    def authenticate(self, request):
        auth_header = request.META.get("HTTP_AUTHORIZATION")

        if not auth_header or not auth_header.startswith("Bearer "):
            return None

        token = auth_header.split(" ")[1]

        try:
            pat = PersonalAccessToken.objects.select_related("user").get(
                token_hash=PersonalAccessToken.hash(token)
            )

            if pat.revoked:
                raise AuthenticationFailed("Token has been revoked")

            if pat.expires_at and pat.expires_at < timezone.now():
                raise AuthenticationFailed("Token has expired")

            if not pat.user.is_active:
                raise AuthenticationFailed("User account is disabled")

            # Update last used timestamp
            pat.last_used_at = timezone.now()
            pat.save(update_fields=["last_used_at"])

            return (pat.user, pat)

        except PersonalAccessToken.DoesNotExist:
            return None
