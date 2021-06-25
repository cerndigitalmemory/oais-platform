import io

import pymarc
import requests
from django.contrib.auth.models import Group, User
from django.http import HttpResponse
from django.shortcuts import redirect
from oais_platform.oais.exceptions import BadRequest, ServiceUnavailable
from oais_platform.oais.models import Archive, Record
from oais_platform.oais.serializers import (ArchiveSerializer, GroupSerializer,
                                            RecordSerializer, UserSerializer)
from oais_platform.oais.sources import InvalidSource, get_source
from rest_framework import permissions, viewsets
from rest_framework.decorators import api_view, permission_classes
from rest_framework.response import Response
from rest_framework.reverse import reverse

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


class ArchiveViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Archive.objects.all()
    serializer_class = ArchiveSerializer
    permission_classes = [permissions.IsAuthenticated]


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def harvest(request, recid, source):
    try:
        url = get_source(source).get_record_url(recid)
    except InvalidSource:
        raise BadRequest("Invalid source")

    record, _ = Record.objects.get_or_create(
        recid=recid,
        source=source,
        defaults={"url": url}
    )

    archive = Archive.objects.create(
        record=record,
        creator=request.user,
    )

    process.delay(archive.id)

    return redirect(
        reverse("archive-detail", request=request, kwargs={"pk": archive.id}))


def task_status(request, task_id):
    task = process.AsyncResult(task_id=task_id)
    return HttpResponse(f"{task.status}, {task.info.get('bagit_res')}")


@api_view()
def search(request, source):
    if "q" not in request.GET:
        raise BadRequest("Missing parameter q")
    query = request.GET["q"]

    try:
        results = get_source(source).search(query)
    except InvalidSource:
        raise BadRequest("Invalid source")

    return Response(results)
