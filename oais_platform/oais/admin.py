from django.contrib import admin
from oais_platform.oais.models import Archive, Step, Collection, Profile, UploadJob

# Register your models here.

admin.site.register(Archive)
admin.site.register(Step)
admin.site.register(Collection)
admin.site.register(Profile)
admin.site.register(UploadJob)
