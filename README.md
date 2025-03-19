# OAIS platform

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Python 3.11](https://img.shields.io/badge/python-3.11-blue.svg)](https://www.python.org/downloads/release/python-3110/)

This is the backend implementation of the OAIS Platform of the CERN Digital Memory, built on Django.

It provides a REST API which can be used to interact with the platform.

The main goals of the platform are:

- Allow users to trigger the _harvesting_ of resources and produce SIPs (using the [bagit-create](https://gitlab.cern.ch/digitalmemory/bagit-create) tool);
- Allow services and users to _deposit_ SIPs and ingest them in the platform;
- Trigger long-term preservation workflows and provide ways to manage them and check on their status;
  - Interface with a distributed deployment of Archivematica;
  - Send SIPs to Archiver.eu platforms and evaluate their interfaces, performance and behaviour on ingestions and processing metadata;
- Send prepared AIPs to the new CERN Tape Archive (CTA);
- Maintain a _registry_ of the successfully harvested and ingested resources, processing and exposing metadata;
- Expose resources on an access system

The platform is designed according to these principles:

- The implementation should reference the OAIS model;
- The products of the archival process (SIPs, AIPs, DIPs) must be able to live on their own;
- The platform target is to coordinate the long term preservation process and provide an orchestration between existing tools and proven frameworks, not to reimplement them or overlap with their responsibilities. The main external components the platform interacts with are:
  - [BagIt-Create](https://gitlab.cern.ch/digitalmemory/bagit-create), a tool able to harvest and pull data from supported upstream sources, creating SIPs compliant to our [specification](https://gitlab.cern.ch/digitalmemory/sip-spec);
  - [Archivematica](https://www.archivematica.org/), responsible of creating the AIPs, running the actual preservation services (re-encodings, file formats conversions, etc);
  - [InvenioRDM](https://inveniordm.web.cern.ch/), a digital repository framework providing an access system to the archived resources (and their artifacts).
  - [CERN Tape Archive (CTA)](https://cta.web.cern.ch/cta/), the final destination of the long term preservation packages.
- The platform must be fully usable through the exposed API surface, enabling any service to integrate a long term preservation strategy to their workflows.
  - A [web interface](https://gitlab.cern.ch/digitalmemory/oais-web) is also provided, allowing users to use the platform through any browser.

## Usage

A public instance of the platform is available over [https://preserve-qa.web.cern.ch/](https://preserve-qa.web.cern.ch/). Swagger API documentation can be found [here](https://preserve-qa.web.cern.ch/api/schema/swagger-ui/).

User documentation is available [here](docs/user.md).

## Run

Here's how you can run a local instance of the platform.

A docker-compose setup is provided in this repository, bringing up the following services:

| Container name | Software   | Role                            | Exposed endpoint               |
| :------------- | :--------- | ------------------------------- | ------------------------------ |
| oais_django    | Django     | Backend API                     | [:8000](http://localhost:8000) |
| oais_celery    | Celery     | Task queue and scheduler (Beat) |                                |
| oais_redis     | Redis      | Broker                          |                                |
| oais_psql      | Postgresql | Database                        |                                |
| oais_pgadmin   | PGAdmin    | Database Browser                | [:5050](http://localhost:5050) |
| oais_nginx     | Nginx      | Reverse Proxy                   | [:80](http://localhost:80)     |

To quickly setup a development instance, featuring hot-reloading on the backend and the frontend:

```bash
# Start by cloning oais-platform
git clone ssh://git@gitlab.cern.ch:7999/digitalmemory/oais-platform.git
# cd into the cloned folder
cd oais-platform
# Bring up the backend and the services:
docker compose up
# From another shell in the same folder and clone there the frontend:
git clone ssh://git@gitlab.cern.ch:7999/digitalmemory/oais-web.git
# cd into the cloned folder
cd oais-web
# Install npm dependencies
npm install --force
# Start an hot-reloading webpack build:
npm run serve
# This will spawn a tab on `localhost:3000`, but we actually want the React app served through nginx, so ignore that
```

The following endpoints are then available, on `localhost`:

- `http://localhost/` - Oais-web React application
- `http://localhost/api` - Base OAIS Platform API endpoint
- `http://localhost/api/schema` - OpenAPI 3 specification of the API
- `http://localhost/api/schema/swagger-ui/` - Swagger UI documentation for the API

Some remarks:

- Node version 14.19.3 or newer is required for for building the web application (use `node -v` to check the current version).
- `npm install --force` is required
- Any changes to the nginx configuration (in nginx/docker.conf) require you to rebuild the image (or shell into the nginx container, edit the file and then `nginx -s reload`)
- Environment variables need to be set in the docker compose or in a `.env.dev` file. **Environment variables from the host environment will be ignored when using compose.**

### Helper commands

A Makefile is included in the repository, providing some utility commands:

- `make admin` creates an admin user (with password `admin`) that can be immediately used to login
- `make reset-db` shuts of the database container, wipes it and brings it up again, resetting the instance to an empty state
- `make shell` will attach to a shell in the Django container

### Django

To run these commands inside a Docker container, run it in the container shell with `docker exec -it oais_django sh`.

```bash
# python manage.py showmigrations
# Prepare migrations
python manage.py makemigrations oais
# Apply migrations
python manage.py migrate
# Create administrator user
DJANGO_SUPERUSER_PASSWORD=root DJANGO_SUPERUSER_USERNAME=root DJANGO_SUPERUSER_EMAIL=root@root.com python3 manage.py createsuperuser --noinput
# Run the application
python manage.py runserver
```

See [troubleshooting](docs/troubleshooting.md) for further instructions on how to maintain an instance and debug issues.

### Run tests

```bash
python manage.py test
```

With docker-compose:

```bash
docker compose down
docker volume prune -y
docker compose -f test-compose.yml up --exit-code-from django
```

Code is formatted using **black** and linted with **flake8**. A VSCode settings file is provided for convenience.

## Configuration

### CERN SSO

To enable the CERN SSO login, set Client ID and Client Secret from your application on https://application-portal.web.cern.ch/. Documentation can be found at https://auth.docs.cern.ch/applications/sso-registration/.

When adding a new "CERN SSO Registration" select OIDC. The redirect URI should be pointing to the `/api/oidc/callback/` endpoint (e.g. `https://<NAME>.web.cern.ch/api/oidc/callback/`) and the Base URL should be something like `https://<NAME>.web.cern.ch`.

```bash
# Secrets for OpenID Connect
export OIDC_RP_CLIENT_ID="Put here the Client ID"
export OIDC_RP_CLIENT_SECRET="Put here the Client Secret"
```

### Sentry

To set up Sentry, set the endpoint with the `SENTRY_DSN` environment variable. To get this value go to your Sentry instance dashboard - Settings - (Select or create a project) - SDK Setup - DSN.

```bash
export SENTRY_DSN="Put here the Sentry SDK client key"
```

### InvenioRDM

To be able to connect the platform with InvenioRDM, create a new API Token in your InvenioRDM instance (Log in - My Account - Applications - Personal access tokens - New token).

```bash
export INVENIO_API_TOKEN=<YOUR_INVENIO_API_TOKEN_HERE>
export INVENIO_SERVER_URL=<YOUR_INVENIO_SERVER_URL_HERE>
```

### FTS

The [CERN FTS client](https://fts.web.cern.ch/fts/) is used to push and retrieve data to the CERN Tape Archive (CTA). It is suggested to set up a Service account for this.

Change your desired `FTS_INSTANCE` to use in the settings. By default the test one is used. `FTS_SOURCE_BASE_PATH` should point to the EOS HTTPS source base path.`FTS_STATUS_INSTANCE`should point to the base url where the job status can be checked for a given ID.

You need a GRID certificate to authenticate. Request one from the [CERN Certification Authority](https://ca.cern.ch/ca/). If using a service account, request permission to obtain a Grid _Robot_ certificate.

Finally, create a new certificate a download the related `.p12` file. We will need to extract the public part and the private one (as passwordless).

```bash
# Get the public part
openssl pkcs12 -in myCert.p12 -clcerts -nokeys -out usercert.pem
# Get the private one. A passphrase is required.
openssl pkcs12 -in myCert.p12 -nocerts -out ./userkey.pem
# Remove the passphrase from the private part
openssl rsa -in userkey.pem -out private.nopwd.key
```

Once you have your final `usercert.pem` and `private.nopwd.key`, set their paths accordingly:

```bash
export FTS_INSTANCE="VALUE"
export FTS_STATUS_INSTANCE="VALUE"
export FTS_SOURCE_BASE_PATH="VALUE"
export FTS_GRID_CERT="VALUE"
export FTS_GRID_CERT_KEY="VALUE"
```

### CTA

Request a namespace on CTA and set the `CTA_BASE_PATH` like this:

```bash
export CTA_BASE_PATH="https://<CTA_ENDPOINT>//eos/<YOUR_CTA_NAMESPACE>"
```

E.g.:

```
root://eosctapublicpps.cern.ch//eos/ctapublicpps/archivetest/digital-memory/
```

Make sure that:

1. The FTS link has correctly mapped the certificate you are planning to use to the service account. This is usually automatic for user Grid certificates but not for Robot ones.
2. The service account has permissions to read and write from the specified CTA space.

## CI/CD

The CI configured on this repository to run the tests on every commit and trigger an upstream deployment.

The platform gets deployed with Helm Charts on a Kubernetes cluster from CERN OpenShift. To learn more, check the [openshift-deploy](https://gitlab.cern.ch/digitalmemory/openshift-deploy) repository.
