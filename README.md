# OAIS platform

Web API, built on Django, managing workflows for the CERN Digital Memory OAIS platform.

Main goals of the platforms are:

- Trigger resource harvesting and produce BagIt packages, using the [bagit-create](https://gitlab.cern.ch/digitalmemory/bagit-create) tool;
- Trigger the preservations and sorrounding workflows;
	- Evaluation of a3m, an alternative Archivemeativa version (gRPC service);
	- Interface with a distributed deployment of Archivematica;
	- Send SIPs to Archiver.eu platforms and evaluate their interfaces, performance and behaviour on ingestions and processing metadata;
- Send prepared AIPs to the new CERN Tape Archive (CTA);
- Maintain a *registry* of the successfully harvested and ingested resources, processing and exposing metadata;
- Expose resources on an access system (powered by Invenio?), exploiting the metadata and revisions features of the CERN AIP specification.

## Requirements

Redis:

```bash
# Install
apt install redis
# Set systemd as the supervisor
#  supervised no -> supervised systemd
vim /etc/redis/redis.conf
# Restart systemd service
systemctl restart redis
# Redis will be up and running at 127.0.0.1:6379
```

Setting up a virtual environment:
```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

BagIt Create tool (development):
```bash
git clone ssh://git@gitlab.cern.ch:7999/digitalmemory/bagit-create.git
# bagit_create should be in the root folder of the project
mv bagit-create/bagit_create/ .
# install BIC requirements
pip install -r bagit_create/requirements.txt
```

## Run

Secrets:
```bash
# Secrets for OpenID Connect
export OIDC_RP_CLIENT_ID="Put here the Client ID"
export OIDC_RP_CLIENT_SECRET="Put here the Client Secret"
```

Django stuff:

```bash
# python manage.py showmigrations
# Prepare migrations
python manage.py makemigrations
# Apply migrations
python manage.py migrate
# Create administrator user
python manage.py createsuperuser
# Run the application
python manage.py runserver
```

On a separate shell, fire up a celery worker:
```bash
celery -A oais_platform.celery worker -l INFO
```

Optionally, start flower this way to get a dashboard to monitor celery tasks:
```bash
flower -A oais_platform.celery --port=5555
```

Exposed endpoints:

- web interface is online at http://localhost:8000/
- flower dashboard at http://localhost:5555/

## Run tests
```bash
python manage.py test
```