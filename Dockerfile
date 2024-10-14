FROM python:3.11-alpine

# Ensure that the python output is sent straight to terminal
ENV PYTHONUNBUFFERED 1

# Django configuration
ENV CELERY_BROKER_URL=
ENV OIDC_RP_CLIENT_ID=
ENV OIDC_RP_CLIENT_SECRET=
ENV SECRET_KEY=

RUN apk add --update \
  build-base \
  cairo \
  cairo-dev \
  cargo \
  freetype-dev \
  gcc \
  gdk-pixbuf-dev \
  gettext \
  jpeg-dev \
  lcms2-dev \
  libffi-dev \
  musl-dev \
  openjpeg-dev \
  openssl-dev \
  pango-dev \
  poppler-utils \
  postgresql-client \
  postgresql-dev \
  py-cffi \
  python3-dev \
  rust \
  tcl-dev \
  tiff-dev \
  tk-dev \
  zlib-dev \
  # to allow pip install dependencies from git repositories
  git \
  # needed to compile M2Crypto, needed for the FTS client
  swig

# Postgresql client
RUN apk add --update --no-cache postgresql-client jpeg-dev 

# Build dependencies
RUN apk add --update --no-cache --virtual .tmp-build-deps \
  gcc libc-dev linux-headers postgresql-dev musl-dev zlib zlib-dev \
  # gssapi header to compile pykerberos
  krb5-dev

# Install python packages
COPY ./requirements.txt /requirements.txt
RUN pip install -r /requirements.txt

# Add CA
RUN apk add --no-cache wget tar curl rpm
RUN mkdir -p /etc/grid-security/certificates

ENV REPOSITORY="https://repository.egi.eu/sw/production/cas/1/current/RPMS/"
ENV CERTIFICATES="Root GridCA"

RUN for CERTIFICATE in $CERTIFICATES; do \
    PACKAGE=$(curl -s $REPOSITORY | grep -oP 'href="\K'"ca_CERN-$CERTIFICATE"'[^"]+\.rpm' | sort -V | tail -n 1); \
      if [ -n "$PACKAGE" ]; then \
          curl -o "/etc/grid-security/certificates/$PACKAGE" "$REPOSITORY/$PACKAGE"; \
          rpm -i "/etc/grid-security/certificates/$PACKAGE"; \
          rm -rf "/etc/grid-security/certificates/$PACKAGE"; \
      else \
          echo "RPM package for ca_CERN-$CERTIFICATE was not found." && exit 1; \
      fi; \
    done

WORKDIR /

# Clean up temporary build dependencies
RUN apk del .tmp-build-deps

RUN mkdir /oais_platform
COPY . /oais_platform/
WORKDIR /oais_platform/