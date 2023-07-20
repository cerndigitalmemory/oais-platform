# Administration Recipes

### Spawn a django shell

A django shell can be spawned by running `python manage.py shell`.

E.g. if you are on the local development setup with compose:

```sh
docker exec -it oais_django python manage.py shell
```

Or, if the instance is on OpenShift, go to **Pods** -> select the "oais-platform" one and on its **Terminal**, run `python manage.py shell`.


### Set every archive as private

```py
from oais_platform.oais.models import Archive
qs = Archive.objects.all()
qs.update(restricted=True)
```

### Create a new user

```py
from django.contrib.auth.models import User
user = User.objects.create_user(username='<USERNAME>',
                                 email='<EMAIL>',
                                 password='<PASSWORD>')
```

### Create or retrieve API token for user

```
make user=USER_NAME add-token
```

### Find all Archive with a specific tag

```python
# Get tag from ID
tag = Collection.objects.get(pk=4)

# Get all the archives tagged
for a in tag.archives.values():
    archive_obj = Archive.objects.get(pk=a['id'])

```

### Delete an Archive and related steps

```python
ARCHIVE_ID_TO_DELETE = 1

a = Archive.objects.get(pk=ARCHIVE_ID_TO_DELETE)

a.last_set = None

a.save()

for s in Step.objects.filter(archive_id=ARCHIVE_ID_TO_DELETE).order_by('-id'):
    s.delete()
```

### Delete every Archive tagged

```python
tag = Collection.objects.get(pk=6)

for a in tag.archives.values():
    print("Deleting Archive ", a["id"])
    archive = Archive.objects.get(pk=a["id"])
    archive.last_step = None
    archive.save()
    for s in Step.objects.filter(archive_id=a["id"]).order_by('-id'):
        print("Deleting Step ", s.id)
        s.delete()
    archive.delete()
```
