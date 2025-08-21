FROM registry.cern.ch/docker.io/cern/alma9-base

# Ensure that the python output is sent straight to terminal
ENV PYTHONUNBUFFERED 1

# Django configuration
ENV CELERY_BROKER_URL=
ENV OIDC_RP_CLIENT_ID=
ENV OIDC_RP_CLIENT_SECRET=
ENV SECRET_KEY=

# Update the base image, install required repositories, and Python 3.11
RUN dnf install -y epel-release && \
    dnf install -y dnf-plugins-core && \
    dnf install -y python3.11 python3.11-devel && \
    ln -sfn /usr/bin/python3.11 /usr/bin/python3 && \
    python3.11 -m ensurepip --upgrade && \
    dnf groupinstall -y "Development Tools" && \
    dnf install -y \
      cairo \
      cairo-devel \
      cargo \
      freetype-devel \
      gdk-pixbuf2 \
      gdk-pixbuf2-devel \
      gettext \
      glibc-devel \
      kernel-headers \
      krb5-devel \
      libjpeg-devel \
      lcms2-devel \
      libffi-devel \
      openjpeg2-devel \
      openssl-devel \
      pango \
      pango-devel \
      poppler-utils \
      postgresql \
      postgresql-devel \
      rust \
      tcl-devel \
      libtiff-devel \
      tk-devel \
      zlib \
      zlib-devel \
      git \
      swig \
      gcc \
      gcc-c++ \
      make \
      cmake \
      boost-devel \
      gfal2 \
      gfal2-devel \
      wget \
      tar \
      bzip2

RUN wget https://archives.boost.io/release/1.89.0/source/boost_1_89_0.tar.bz2
RUN tar xf boost_1_89_0.tar.bz2

WORKDIR /boost_1_89_0
RUN ./bootstrap.sh --with-python=/usr/bin/python3.11 && \
  ./b2 --with-python && \
  ./b2 install --with-python
WORKDIR /

# Install python packages
COPY ./requirements.txt /requirements.txt
RUN pip3 install wheel && pip3 install -r /requirements.txt

COPY docker/carepo.repo /etc/yum.repos.d/
RUN dnf install -y ca_CERN-Root-2 ca_CERN-GridCA && dnf clean -y all

RUN mkdir /oais_platform
COPY . /oais_platform/
WORKDIR /oais_platform/