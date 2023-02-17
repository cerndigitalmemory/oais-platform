# Administration Recipes

A django shell can be spawned with

```sh
docker exec -it oais_django python manage.py shell
```

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
