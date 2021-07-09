from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('oais', '0002_create_archive_alter_record'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='archive',
            options={'permissions': [('can_approve_archive', 'Can approve an archival request'), ('can_reject_archive', 'Can reject an archival request')]},
        ),
    ]
