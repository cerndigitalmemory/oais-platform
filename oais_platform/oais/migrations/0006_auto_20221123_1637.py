# Generated by Django 3.2.4 on 2022-11-23 16:37

import django.contrib.postgres.fields
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('oais', '0005_profile_claims'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='profile',
            name='claims',
        ),
        migrations.AddField(
            model_name='profile',
            name='cern_roles',
            field=django.contrib.postgres.fields.ArrayField(base_field=models.CharField(max_length=500), blank=True, default=list, size=None),
        ),
    ]
