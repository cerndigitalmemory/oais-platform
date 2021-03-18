# OAIS platform

```bash
git clone ssh://git@gitlab.cern.ch:7999/digitalmemory/oais-platform-d.git

python -m venv env
source env/bin/activate
pip install -r requirements.txt

python manage.py makemigrations
python manage.py migrate
python manage.py createsuperuser
python manage.py runserver
```