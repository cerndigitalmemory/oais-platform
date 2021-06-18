import io

import pymarc
import requests
from django.contrib.auth.models import Group, User
from django.http import HttpResponse
from oais_platform.oais.exceptions import BadRequest, ServiceUnavailable
from oais_platform.oais.models import Record
from oais_platform.oais.serializers import (GroupSerializer, RecordSerializer,
                                            UserSerializer)
from rest_framework import permissions, viewsets
from rest_framework.decorators import api_view
from rest_framework.response import Response

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
