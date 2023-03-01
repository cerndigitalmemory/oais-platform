# This makefile provides some shortcuts to help manage a local instance of the platform 
# brought up with the provided docker compose setup

# Create a sample admin account with superuser privileges with the credentials
# admin:admin
admin:
	docker exec -e DJANGO_SUPERUSER_PASSWORD=admin oais_django python3 manage.py createsuperuser --noinput --username admin --email root@root.com

# Reset the database, restarting all the containers and bringing the instance back up
reset-db:
	docker compose stop
	docker compose down
	docker volume rm oais-platform_postgres -f
	docker compose up

# Show (and follow) logs of the Celery container
logs-celery:
	docker logs oais_celery -f

# Show (and follow) logs of the Django container
logs-django:
	docker logs oais_django -f

# Attach to a shell inside the Django container
shell:
	docker exec -it oais_django sh

# Prepare migrations and apply them
migrations:
	docker exec oais_django python manage.py makemigrations
	docker exec oais_django python manage.py migrate

# Cleans up logs and local data (e.g. SIPs)
# should be run with a "reset-db"
clean:
	rm -rf oais-data/*
	rm *.tmp

test:
	docker compose -f test-compose.yml up --exit-code-from django

add-token:
	docker exec oais_django python manage.py drf_create_token $(foo)
