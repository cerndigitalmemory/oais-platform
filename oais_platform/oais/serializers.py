from django.contrib.auth.models import Group, User
from oais_platform.oais.models import Archive, Record
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


class RecordSerializer(serializers.ModelSerializer):
    class Meta:
        model = Record
        fields = ["id", "url", "recid", "source"]


class ArchiveSerializer(serializers.ModelSerializer):
    creator = UserSerializer()
    record = RecordSerializer()

    class Meta:
        model = Archive
        fields = ["id", "record", "creator",
                  "creation_date", "celery_task_id", "status", "stage"]

class LoginSerializer(serializers.Serializer):
    username = serializers.CharField(max_length=150)
    password = serializers.CharField(max_length=128)