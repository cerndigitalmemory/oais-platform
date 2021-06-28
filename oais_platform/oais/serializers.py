from django.contrib.auth.models import Group, User
from oais_platform.oais.models import Archive, Record
from rest_framework import serializers


class UserSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = User
        fields = ['url', 'username', 'email', 'groups']


class GroupSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Group
        fields = ['url', 'name']


class RecordSerializer(serializers.HyperlinkedModelSerializer):
    class Meta:
        model = Record
        fields = ["url", "id", "recid", "source"]


class ArchiveSerializer(serializers.HyperlinkedModelSerializer):
    creator = UserSerializer()
    record = RecordSerializer()

    class Meta:
        model = Archive
        fields = ["url", "id", "record", "creator",
                  "creation_date", "celery_task_id", "status"]
