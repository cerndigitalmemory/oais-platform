from re import S
from django.contrib.auth.models import Group, User
from rest_framework.fields import IntegerField
from oais_platform.oais.models import Archive, Step
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
    # creator = UserSerializer()

    class Meta:
        model = Archive
        fields = [
            "id",
            "source_url",
            "recid",
            "source",
            "timestamp",
            "current_status",
            "path_to_sip",
        ]

    def get_last_step(self, instance):
        last_step = instance.steps.all().order_by("-start_date")[0]
        return last_step


class LoginSerializer(serializers.Serializer):
    username = serializers.CharField(max_length=150)
    password = serializers.CharField(max_length=128)
