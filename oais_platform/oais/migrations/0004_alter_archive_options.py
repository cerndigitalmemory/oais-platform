from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('oais', '0003_alter_archive_options'),
    ]

    operations = [
        migrations.AlterModelOptions(
            name='archive',
            options={'permissions': [('can_access_all_archives', 'Can access all the archives'), ('can_approve_archive', 'Can approve an archival request'), ('can_reject_archive', 'Can reject an archival request')]},
        ),
    ]
