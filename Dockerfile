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
RUN apk add ca_CERN-Root-2 --repository=https://linuxsoft.cern.ch/mirror/repository.egi.eu/sw/production/cas/1/current/ && apk cache clean

WORKDIR /

# Clean up temporary build dependencies
RUN apk del .tmp-build-deps

RUN mkdir /oais_platform
COPY . /oais_platform/
WORKDIR /oais_platform/