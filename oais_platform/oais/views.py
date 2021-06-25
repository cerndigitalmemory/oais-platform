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
    if source == "cds-test":
        url = f"https://cds-test.cern.ch/record/{recid}"
    elif source == "cds":
        url = f"https://cds.cern.ch/record/{recid}"

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

    results = None
    if source == "cds-test":
        results = search_cds("https://cds-test.cern.ch", source, query)
    elif source == "cds":
        results = search_cds("https://cds.cern.ch", source, query)

    if not results:
        raise BadRequest("Invalid source")

    return Response(results)


def search_cds(baseUrl, source, query):
    try:
        req = requests.get(baseUrl + "/search",
                           params={"p": query, "of": "xm"})
    except:
        raise ServiceUnavailable("Cannot perform search")

    if not req.ok:
        raise ServiceUnavailable(
            f"Search failed with error code {req.status_code}")

    # Parse MARC XML
    records = pymarc.parse_xml_to_array(io.BytesIO(req.content))
    results = []
    for record in records:
        recid = record["001"].value()

        authors = []
        for author in record.get_fields("100", "700"):
            authors.append(author["a"])

        results.append({
            "url": f"{baseUrl}/record/{recid}",
            "recid": recid,
            "title": record.title(),
            "authors": authors,
            "source": source
        })

    return results
