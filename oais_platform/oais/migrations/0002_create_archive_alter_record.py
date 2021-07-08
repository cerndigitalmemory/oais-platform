from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion
import django.utils.timezone


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('oais', '0001_initial'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='record',
            unique_together={('recid', 'source')},
        ),
        migrations.CreateModel(
            name='Archive',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('creation_date', models.DateTimeField(default=django.utils.timezone.now)),
                ('celery_task_id', models.CharField(default=None, max_length=50, null=True)),
                ('status', models.IntegerField(choices=[(1, 'Pending'), (2, 'In Progress'), (3, 'Failed'), (4, 'Completed'), (5, 'Waiting Approval'), (6, 'Rejected')], default=5)),
                ('creator', models.ForeignKey(null=True, on_delete=django.db.models.deletion.PROTECT, related_name='archives', to=settings.AUTH_USER_MODEL)),
                ('record', models.ForeignKey(on_delete=django.db.models.deletion.PROTECT, related_name='archives', to='oais.record')),
            ],
        ),
    ]
