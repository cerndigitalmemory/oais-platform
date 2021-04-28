from django.shortcuts import render

from django.http import HttpResponse
from django.contrib.auth.models import User, Group
from oais_platform.oais.models import Record
from rest_framework import viewsets
from rest_framework import permissions
from oais_platform.oais.serializers import (
    UserSerializer,
    GroupSerializer,
    RecordSerializer,
)

from .tasks import process


class UserViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows users to be viewed or edited.
    """

    queryset = User.objects.all().order_by("-date_joined")
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]


class GroupViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """

    queryset = Group.objects.all()
    serializer_class = GroupSerializer
    permission_classes = [permissions.IsAuthenticated]


class RecordViewSet(viewsets.ModelViewSet):
    """
    API endpoint that allows records to be viewed or edited.
    """

    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    permission_classes = [permissions.IsAuthenticated]


def harvest(request, rec_id, source):
    task_id = process.apply_async(args=(rec_id, source,))
    return HttpResponse(f"You requested recid {rec_id} from {source}. Celery task is {task_id}")


def task_status(request, task_id):
    task = process.AsyncResult(task_id=task_id)
    return HttpResponse(f"{task.status}, {task.info.get('bagit_res')}")
