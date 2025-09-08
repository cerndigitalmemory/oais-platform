FROM cern/alma9-base

# Ensure that the python output is sent straight to terminal
ENV PYTHONUNBUFFERED 1

# Install Python 3.11 and essential build tools
RUN dnf install -y epel-release && \
    dnf install -y python3.11 python3.11-devel && \
    ln -sfn /usr/bin/python3.11 /usr/bin/python3 && \
    python3.11 -m ensurepip --upgrade && \
    dnf install -y \
      gcc \
      gcc-c++ \
      make \
      cmake \
      gfal2 \
      gfal2-devel \
      gfal2-plugin-http \
      wget \
      tar \
      bzip2 && \
    dnf clean -y all

# Install Boost required for gfal2
RUN wget https://archives.boost.io/release/1.89.0/source/boost_1_89_0.tar.bz2 && \
    tar xf boost_1_89_0.tar.bz2
WORKDIR /boost_1_89_0
RUN ./bootstrap.sh --with-python=/usr/bin/python3.11 && \
  ./b2 --with-python && \
  ./b2 install --with-python
WORKDIR /

# Install Python packages
COPY ./scripts/requirements.txt /scripts/requirements.txt
RUN pip3 install --no-cache-dir wheel && pip3 install --no-cache-dir -r /scripts/requirements.txt

COPY docker/carepo.repo /etc/yum.repos.d/
RUN dnf install -y ca_CERN-Root-2 ca_CERN-GridCA && dnf clean -y all

COPY ./scripts/browse_tape.py /scripts/browse_tape.py
CMD ["bash"]