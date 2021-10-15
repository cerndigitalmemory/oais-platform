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

On a Debian system:

```bash
# Required to build `psycopg2`
apt install libpq-dev python3-dev
```

Install Redis

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

Set up a virtual environment and install python requirements

```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Alternatively, a Docker Compose setup is provided in this repository.

## Configuration

### CERN SSO

To enable the CERN SSO login, set Client ID and Client Secret from your application on https://application-portal.web.cern.ch/. Documentation can be found at https://auth.docs.cern.ch/applications/sso-registration/.

```bash
# Secrets for OpenID Connect
export OIDC_RP_CLIENT_ID="Put here the Client ID"
export OIDC_RP_CLIENT_SECRET="Put here the Client Secret"
```

### Sentry

To set up Sentry, set the endpoint with the `SENTRY_DSN` environment variable. To get this value go to your Sentry instance dashboard - Settings - (Select or create a project) - SDK Setup - DSN.

```
export SENTRY_DSN="Put here the Sentry SDK client key"
```

## Run

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
