# Troubleshooting

Here's a quick overview on how to troubleshoot the OAIS platform.

Containers overview:

| Container name | Software   | Role                            | Exposed endpoint               |
| :------------- | :--------- | ------------------------------- | ------------------------------ |
| oais_django    | Django     | Backend API                     | [:8000](http://localhost:8000) |
| oais_celery    | Celery     | Task queue and scheduler (Beat) |                                |
| oais_redis     | Redis      | Broker                          |                                |
| oais_psql      | Postgresql | Database                        |                                |
| oais_pgadmin   | PGAdmin    | Database Browser                | [:5050](http://localhost:5050) |

- Bring up the containers (start everything), showing aggregated logs
  `docker-compose up`
  Stop containers with <kbd>CTRL</kbd>+<kbd>C</kbd>
- Bring up the containers in detached mode (no logs)
  `docker-compose up -d`
- Show logs of single container
  `docker-compose logs <CONTAINER_NAME>`
  Use `-f` to keep following the logs
- Remove containers
  `docker-compose down`
- Clean up volumes (e.g. to reset the database)
  `docker volume prune -f`
- Run command in container
  `docker exec -it <CONTAINER_NAME> <COMMAND>`
- Open shell in container
  `docker exec -it <CONTAINER_NAME> sh`
- Rebuild images (e.g. when changing the Dockerfiles or requirements.txt)
  `docker-compose build`
- Rebuild images skipping cache. Can take a lot of time.
  Sometimes you need it when upgrading pip dependencies that are pointing to git repositories, e.g. bagit-create.
  `docker-compose build --no-cache`

To keep in mind:

- Modifications to tasks.py (and in general anything executed by Celery) may require a restart of the Celery runner
  `docker restart oais_celery`
- Some changes (e.g. the change of a requirement) require the oais_django image to be rebuilt.
  `docker-compose build`
- Sometimes, the oais_django container may start before the database is ready. If it can't be started because of a failed connection to postgres, restart it manually:
  `docker start oais_django`

Settings:

To show the complete list of configuration values (settings.py) Django is using, run

```bash
python manage.py diffsettings --all
```

The "priority" order of settings is:

1. local_settings (if the file exists, e.g. we use it in OpenShift deployments)
2. Environment variables
3. Defaults in settings.py

To reset your instance:

```bash
docker-compose down
docker volume prune
docker-compose up
# sometimes django may need to be restarted manually..
docker start oais_django
# create a new superuser (with user and pass "admin") without entering manually the values
docker exec -e DJANGO_SUPERUSER_PASSWORD=admin oais_django python3 manage.py createsuperuser --noinput --username admin --email root@root.com
```

### Django

If you need to create migrations but you can't get django up through docker (e.g. failing because of model changes), create a local virtual env to run `manage.py`:

```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
python manage.py makemigrations
# on the next start of the container a "migrate" command is run
```

- Collect static files (to correctly see the Django admin panel)
  `python manage.py collectstatic`

### Postgres/database:

- Browse database using PGAdmin
  Open [localhost:5050](http://localhost:5050) and create a new connection, with address `db` and the password provided in the docker-compose.yml (by default `overwritethisinprod!`).

### Celery

- Celery: set log level to "DEBUG" instead of "INFO" in the worker:
  `celery -A oais_platform.celery worker -l INFO` -> `celery -A oais_platform.celery worker -l DEBUG`

### Locally testing the "Announce" feature

Move (or create) a SIP directly in the oais-platform main path. It is mounted by default as /oais-platform in the django and celery containers.

So e.g. if you have `/home/avivace/dm/oais-platform/sip::cds::2798105::1673532831` announce it as `/oais_platform/sip::cds::2798105::1673532831`.
