admin:
	docker exec -e DJANGO_SUPERUSER_PASSWORD=admin oais_django python3 manage.py createsuperuser --noinput --username admin --email root@root.com

reset-db:
	docker-compose stop
	docker-compose down
	docker volume rm oais-platform_postgres -f

logs-celery:
	docker logs oais_celery -f

logs-django:
	docker logs oais_django -f
