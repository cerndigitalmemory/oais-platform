# OAIS platform

Getting ready:

```bash
git clone ssh://git@gitlab.cern.ch:7999/digitalmemory/oais-platform-d.git

# Set up virtual env and install requirements
python -m venv env
source env/bin/activate
pip install -r requirements.txt

# 
python manage.py makemigrations
#
python manage.py migrate
# Create administrator user
python manage.py createsuperuser
# Run the application
python manage.py runserver
```

API web interface is online at http://localhost:8000/
