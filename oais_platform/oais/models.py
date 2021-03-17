from django.db import models

# Create your models here.

class Record(models.Model):
    url = models.CharField(max_length=100)
    recid = models.CharField(max_length=50)
    source = models.CharField(max_length=50)