# OAIS platform

Web API, built on Django, managing workflows for the CERN Digital Memory OAIS platform.

Main goals of the platforms are:

- Trigger resource harvesting and produce BagIt packages, using the [bagit-create](https://gitlab.cern.ch/digitalmemory/bagit-create) tool;
- Trigger enduro/Archivematica ingestions and sorrounding workflows;
	- Evaluation of a3m, an alternative Archivemeativa version (gRPC service);
	- Interface with a distributed deployment of Archivematica;
- Send final data packages to the new CERN Tape Archive (CTA);
- Implement workflows to send final data to Archiver.eu platforms and evaluate their interfaces, performance and behaviour on processing metadata;
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
# Up and running at 127.0.0.1:6379
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
mv bagit-create/bagit-create/bagit_create bagit_create
# install BIC requirements
pip install -r bagit_create/requirements.txt
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

API web interface is online at http://localhost:8000/
