# Administration Recipes


### Set every archive as private

```
python manage.py shell
from oais_platform.oais.models import Archive
qs = Archive.objects.all()
qs.update(restricted=True)
```
