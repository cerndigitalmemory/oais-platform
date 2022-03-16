from re import S

from django.contrib.auth.models import Group, User
from oais_platform.oais.models import Archive, Collection, Step, Record
from rest_framework import serializers
from rest_framework.fields import IntegerField


class UserSerializer(serializers.ModelSerializer):
    permissions = serializers.SerializerMethodField()

    class Meta:
        model = User
        fields = ["id", "username", "permissions", "first_name", "last_name"]

    def get_permissions(self, obj):
        return obj.get_all_permissions()


class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ["url", "name"]


class StepSerializer(serializers.ModelSerializer):
    archive = serializers.IntegerField(source="archive.id")

    class Meta:
        model = Step
        fields = [
            "id",
            "archive",
            "name",
            "start_date",
            "finish_date",
            "status",
            "celery_task_id",
            "input_data",
            "input_step",
            "output_data",
        ]


class ArchiveSerializer(serializers.ModelSerializer):
    creator = UserSerializer()

    class Meta:
        model = Archive
        fields = [
            "id",
            "source_url",
            "recid",
            "source",
            "creator",
            "timestamp",
            "last_step",
            "path_to_sip",
            "next_steps",
            "manifest",
        ]

    def get_last_step(self, instance):
        last_step = instance.steps.all().order_by("-start_date")[0]
        return last_step


class CollectionSerializer(serializers.ModelSerializer):
    archives = ArchiveSerializer(many=True)
    creator = UserSerializer()

    class Meta:
        model = Collection
        fields = [
            "id",
            "title",
            "description",
            "creator",
            "timestamp",
            "last_modification_date",
            "archives",
        ]


class RecordSerializer(serializers.ModelSerializer):
    tags = CollectionSerializer(many=True)
    record_creator = UserSerializer()

    class Meta:
        model = Record
        fields = [
            "id",
            "source_url",
            "title",
            "recid",
            "source",
            "record_creator",
            "timestamp",
            "authors",
            "tags",
        ]


class LoginSerializer(serializers.Serializer):
    username = serializers.CharField(max_length=150)
    password = serializers.CharField(max_length=128)
