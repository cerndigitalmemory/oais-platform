"""
Django settings for oais_platform project.

For more information on this file, see
https://docs.djangoproject.com/en/3.1/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.1/ref/settings/

This file provides the base configuration values for the
various components of the project.

Some of them can be easily customised by setting
environment variables, however, for a full control approach
over this file (e.g. deployments), you can edit the
`local_settings/__init__.py` file.

That package is loaded at the end of this file and
everything defined in local_settings *will override*
everything specified here, including values set via
environment values.

"""
from os import environ
from pathlib import Path

import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration

# Build paths inside the project like this: BASE_DIR / 'subdir'.
BASE_DIR = Path(__file__).resolve().parent.parent

TESTING = False

STATIC_ROOT = environ.get("DJANGO_STATIC_ROOT", "oais-web/build/static")

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.1/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = "REPLACE_ME"

# SECURITY WARNING: don't run with debug turned on in production!
DEBUG = True

ALLOWED_HOSTS = []

# Celery

CELERY_BROKER_URL = environ.get("CELERY_BROKER_URL", "redis://127.0.0.1:6379/0")
CELERY_RESULT_BACKEND = environ.get("CELERY_RESULT_BACKEND", "redis://127.0.0.1:6379/0")
CELERY_ACCEPT_CONTENT = ["application/json"]
CELERY_RESULT_SERIALIZER = "json"
CELERY_TASK_SERIALIZER = "json"

# OpenID Connect
OIDC_RP_CLIENT_ID = environ.get("OIDC_RP_CLIENT_ID")
# SECURITY WARNING: the client secret must be kept secret!
OIDC_RP_CLIENT_SECRET = environ.get("OIDC_RP_CLIENT_SECRET")
OIDC_OP_AUTHORIZATION_ENDPOINT = (
    "https://keycloak-qa.cern.ch/auth/realms/cern/protocol/openid-connect/auth"
)
OIDC_OP_TOKEN_ENDPOINT = (
    "https://keycloak-qa.cern.ch/auth/realms/cern/protocol/openid-connect/token"
)
OIDC_OP_USER_ENDPOINT = (
    "https://keycloak-qa.cern.ch/auth/realms/cern/protocol/openid-connect/userinfo"
)
OIDC_OP_JWKS_ENDPOINT = (
    "https://keycloak-qa.cern.ch/auth/realms/cern/protocol/openid-connect/certs"
)
OIDC_RP_SIGN_ALGO = "RS256"
# Path to redirect to on successful login.
# This is used to fetch the user information from the SPA.
LOGIN_REDIRECT_URL = "/index.html#/login/callback"


AUTHENTICATION_BACKENDS = [
    "oais_platform.oais.auth.CERNAuthenticationBackend",
    "django.contrib.auth.backends.ModelBackend",
    "guardian.backends.ObjectPermissionBackend",
]


# Application definition

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "rest_framework",
    "oais_platform",
    "oais_platform.oais",
    "corsheaders",
    "django_celery_beat",
    "drf_spectacular",
    "drf_spectacular_sidecar",
    "guardian",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "corsheaders.middleware.CorsMiddleware",
]

ROOT_URLCONF = "oais_platform.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

WSGI_APPLICATION = "oais_platform.wsgi.application"


# Database
# https://docs.djangoproject.com/en/3.1/ref/settings/#databases

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": environ.get("DB_HOST"),
        "NAME": environ.get("DB_NAME"),
        "USER": environ.get("DB_USER"),
        "PASSWORD": environ.get("DB_PASS"),
        "TEST": {
            "NAME": "mytestdatabase",
        },
    }
}


# Password validation
# https://docs.djangoproject.com/en/3.1/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        "NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.CommonPasswordValidator",
    },
    {
        "NAME": "django.contrib.auth.password_validation.NumericPasswordValidator",
    },
]


# Internationalization
# https://docs.djangoproject.com/en/3.1/topics/i18n/

LANGUAGE_CODE = "en-us"

TIME_ZONE = "UTC"

USE_I18N = True

USE_L10N = True

USE_TZ = True


# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.1/howto/static-files/

STATIC_URL = "static/"

REST_FRAMEWORK = {
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.PageNumberPagination",
    "PAGE_SIZE": 10,
    "DEFAULT_AUTHENTICATION_CLASSES": [
        "rest_framework.authentication.SessionAuthentication",
    ],
    "DEFAULT_SCHEMA_CLASS": "drf_spectacular.openapi.AutoSchema",
}

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# SPECTACULAR

SPECTACULAR_SETTINGS = {
    "TITLE": "OAIS Platform API",
    "DESCRIPTION": "CERN Digital Memory platform API documentation",
    "VERSION": "0.1",
    # OTHER SETTINGS
}

# CORS

CORS_ORIGIN_ALLOW_ALL = True

# Sentry
sentry_sdk.init(
    dsn=environ.get("SENTRY_DSN"),
    integrations=[DjangoIntegration()],
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    # We recommend adjusting this value in production,
    traces_sample_rate=1.0,
    # If you wish to associate users to errors (assuming you are using
    # django.contrib.auth) you may enable sending PII data.
    send_default_pii=True,
    # By default the SDK will try to use the SENTRY_RELEASE
    # environment variable, or infer a git commit
    # SHA as release, however you may want to set
    # something more human-readable.
    # release="myapp@1.0.0",
)

# ARCHIVEMATICA SETTINGS

# add the URL archivematica is running, username and password
AM_URL = "http://umbrinus.cern.ch:62080"
AM_USERNAME = "test"
AM_API_KEY = "test"
AM_SS_URL = "http://umbrinus.cern.ch:62081"
AM_SS_USERNAME = "test"
AM_SS_API_KEY = "test"

# add the UUID of the transfer source
AM_TRANSFER_SOURCE = "42e55273-87cb-4724-9748-1e6d5a1affa6"

# Absolute directory of the source folder for archivematica
AM_ABS_DIRECTORY = "/root/oais-platform/oais-data"
# Directory that Archivematica "sees" on the local system
AM_REL_DIRECTORY = "/home/archivematica/archivematica-sampledata/oais-data"


# Bagit Create Settings

# Path where bagitcreate exports data (using the target option)
BIC_UPLOAD_PATH = "oais-data"

# Base URL that serves the packages
FILES_URL = "https://oais.web.cern.ch/"
# Path where the AIPs will be served from
AIP_UPSTREAM_BASEPATH = "/oais-data/aip/"
# Path where the SIPs will be served from
SIP_UPSTREAM_BASEPATH = "/oais-data/sip/"

# Import local settings
from oais_platform.local_settings import *  # noqa
