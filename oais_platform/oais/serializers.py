from django.contrib.auth.models import Group, User
from oais_platform.oais.models import Archive, ArchiveStatus, Record
from rest_framework import serializers
from rest_framework.serializers import ValidationError


class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = User
        fields = ["id", "username"]


class GroupSerializer(serializers.ModelSerializer):
    class Meta:
        model = Group
        fields = ['url', 'name']


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
                  "creation_date", "celery_task_id", "status"]

    def update(self, instance, validated_data):
        new_status = validated_data.pop("status", instance.status)
        if len(validated_data) != 0:
            raise ValidationError("Only status can be updated")

        if new_status != instance.status:
            if instance.status != ArchiveStatus.WAITING_APPROVAL:
                raise ValidationError("Archive is not waiting for approval")
            if new_status not in (ArchiveStatus.PENDING, ArchiveStatus.REJECTED):
                raise ValidationError(
                    "New status is not 'pending' or 'rejected'")
            instance.status = new_status

        instance.save()
        return instance
