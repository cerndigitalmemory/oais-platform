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
  `docker compose up`
  Stop containers with <kbd>CTRL</kbd>+<kbd>C</kbd>
- Bring up the containers in detached mode (no logs)
  `docker compose up -f`
- Show logs of single container
  `docker-compose logs <CONTAINER_NAME>`
  Use `-f` to keep following the logs
- Remove containers
  `docker-compose down`
- Clean up volumes (e.g. to reset the database)
  `docker volume prune -y`
- Run command in container
  `docker exec it <CONTAINER_NAME> <COMMAND>`
- Open shell in container
  `docker exec it <CONTAINER_NAME> sh`
- Browse database using PGAdmin
  Open [localhost:5050](http://localhost:5050) and create a new connection, with address `db` and the password provided in the docker-compose.yml (by default `overwritethisinprod!`).
- Rebuild images (e.g. when changing any of the Dockerfiles)
  `docker-compose build`