# Generated by Django 4.2.1 on 2024-04-17 12:53

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('oais', '0011_alter_archive_state_add_last_completed_step'),
    ]

    operations = [
        migrations.AddConstraint(
            model_name='resource',
            constraint=models.UniqueConstraint(fields=('source', 'recid'), name='resource_source_recid_unique'),
        ),
    ]
