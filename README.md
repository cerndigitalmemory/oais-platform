# OAIS platform

Web API, built on Django, managing workflows for the CERN Digital Memory OAIS platform.

Main goals of the platforms are:

- Trigger resource harvesting and produce BagIt packages, using the [bagit-create](https://gitlab.cern.ch/digitalmemory/bagit-create) tool;
- Trigger the preservations and sorrounding workflows;
  - Evaluation of a3m, an alternative Archivemeativa version (gRPC service);
  - Interface with a distributed deployment of Archivematica;
  - Send SIPs to Archiver.eu platforms and evaluate their interfaces, performance and behaviour on ingestions and processing metadata;
- Send prepared AIPs to the new CERN Tape Archive (CTA);
- Maintain a _registry_ of the successfully harvested and ingested resources, processing and exposing metadata;
- Expose resources on an access system (powered by Invenio?), exploiting the metadata and revisions features of the CERN AIP specification.

## Run

A docker-compose setup is provided in this repository, bringing up the following services:

| Container name | Software   | Role                            | Exposed endpoint               |
| :------------- | :--------- | ------------------------------- | ------------------------------ |
| oais_django    | Django     | Backend API                     | [:8000](http://localhost:8000) |
| oais_celery    | Celery     | Task queue and scheduler (Beat) |                                |
| oais_redis     | Redis      | Broker                          |                                |
| oais_psql      | Postgresql | Database                        |                                |
| oais_pgadmin   | PGAdmin    | Database Browser                | [:5050](http://localhost:5050) |
| oais_nginx     | Nginx      | Reverse Proxy                   | [:80](http://localhost:80)     |

To quickly setup a development instance, featuring hot-reloading on the backend:

```bash
# Start by cloning oais-platform
git clone ssh://git@gitlab.cern.ch:7999/digitalmemory/oais-platform.git
# Inside it, clone oais-web
git clone ssh://git@gitlab.cern.ch:7999/digitalmemory/oais-web.git oais-platform/oais-web
# Build the web application
cd oais-platform/oais-web
npm install
npm run build
# Go back to the oais-platform folder and launch the docker compose setup
cd ..
docker-compose up
```

If you also want the React application to hot-reload on file modifications, instead of running `npm run build`, keep a shell open and run `npm run serve`.

The following endpoints are then available, on `localhost`:

- `/` - Oais-web React application
- `/api` - Base OAIS Platform API endpoint
- `/api/schema` - OpenAPI 3 specification of the API
- `http://localhost/api/schema/swagger-ui/` - Swagger UI documentation for the API

See [troubleshooting](docs/troubleshooting.md) for more instructions on how to see logs and run commands in the single containers.

### Django

To run these commands inside a Docker container, prefix them with `docker exec -it oais_django`.

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

### Run tests

```bash
python manage.py test
```

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

```bash
export SENTRY_DSN="Put here the Sentry SDK client key"
```
