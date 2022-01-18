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

## Run

A docker-compose setup is provided in this repository, bringing up the following services:

| Container name | Software   | Role                            | Exposed endpoint                       |
|:------------ |:---------- | ------------------------------- | ------------------------------ |
| oais_django  | Django     | Backend API                     | [:8000](http://localhost:8000) |
| oais_celery  | Celery     | Task queue and scheduler (Beat) |                                |
| oais_redis   | Redis      | Broker                          |                                |
| oais_psql    | Postgresql | Database                        |                           |
| oais_pgadmin | PGAdmin    | Database Browser                | [:5050](http://localhost:5050) |

Run `docker-compose up` to bring up the full stack. The django app will auto reload on file modifications.

To also serve the frontend application, copy a build of [oais-web](https://gitlab.cern.ch/digitalmemory/oais-web) in the "static" folder and uncomment the last lines of `oais_platform/urls.py`.

See [troubleshooting](docs/troubleshooting.md) for more instructions on how to see logs and run commands in the single containers.

### Django

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
