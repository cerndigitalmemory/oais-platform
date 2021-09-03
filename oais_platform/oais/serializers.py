from re import S
from django.contrib.auth.models import Group, User
from rest_framework.fields import IntegerField
from oais_platform.oais.models import Archive, Record, Job
from rest_framework import serializers


class UserSerializer(serializers.ModelSerializer):
    permissions = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ["id", "username", "permissions"]

    def get_permissions(self, obj):
        return obj.get_all_permissions()


class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ['url', 'name']


class RecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = Record
        fields = ["id", "url", "recid", "source"]

class JobSerializer(serializers.ModelSerializer):
    archive_id = serializers.IntegerField(source='archive.id')

    class Meta:
        model = Job
        fields = ["id", "archive_id", "celery_task_id",
                  "start_date", "finish_date", "status", "stage"]

class ArchiveSerializer(serializers.ModelSerializer):
    creator = UserSerializer()
    record = RecordSerializer()
    stage = serializers.SerializerMethodField()

    class Meta:
        model = Archive
        fields = ["id", "record", "creator",
                  "creation_date", "celery_task_id", "status", "stage"]

    # Getting stage from last job
    def get_stage(self, instance):
        last_job = instance.jobs.all().order_by('-start_date')[0]
        return last_job.stage


class LoginSerializer(serializers.Serializer):
    username = serializers.CharField(max_length=150)
    password = serializers.CharField(max_length=128)