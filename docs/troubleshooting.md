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

To reset your instance:

```bash
docker-compose down
docker volume purge
docker-compose up
# sometimes django needs to be restarted manually..
docker start oais_django
# create a new superuser (user and pass admin) without entering manually the values
docker exec -e DJANGO_SUPERUSER_PASSWORD=admin oais_django python3 manage.py createsuperuser --noinput --username admin --email root@root.com
```

Django:

If you need to create migrations, create a local virtual env to run `manage.py`:

```bash
python -m venv env
source env/bin/activate
pip install -r requirements.txt
python manage.py makemigrations
# on the next start of the container a "migrate" command is run
```

- Collect static files (to correctly see the Django admin panel)
  `python manage.py collectstatic`

Postgres/database:

- Browse database using PGAdmin
  Open [localhost:5050](http://localhost:5050) and create a new connection, with address `db` and the password provided in the docker-compose.yml (by default `overwritethisinprod!`).

Celery:

- Celery: set log level to "DEBUG" instead of "INFO" in the worker:
  `celery -A oais_platform.celery worker -l INFO` -> `celery -A oais_platform.celery worker -l DEBUG`

OpenSearch:

- Create indices
  `python manage.py opensearch index create`
- Populate indices
  `python3 manage.py opensearch document index`
