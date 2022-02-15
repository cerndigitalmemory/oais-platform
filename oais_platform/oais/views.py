import collections
import json
import logging
import os
import subprocess
import time
import zipfile

from django.contrib import auth
from django.contrib.auth.models import Group, User
from django.db import transaction
from django.db.models import base
from django.shortcuts import redirect
from oais_platform.oais.exceptions import BadRequest
from oais_platform.oais.mixins import PaginationMixin
from oais_platform.oais.models import Archive, Collection, Status, Step, Steps
from oais_platform.oais.permissions import (
    filter_archives_by_user_perms,
    filter_steps_by_user_perms,
    filter_collections_by_user_perms,
)
from oais_platform.oais.serializers import (
    ArchiveSerializer,
    CollectionSerializer,
    GroupSerializer,
    LoginSerializer,
    StepSerializer,
    UserSerializer,
    CollectionSerializer,
)
from oais_platform.oais.sources import InvalidSource, get_source
from rest_framework import permissions, viewsets
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response
from rest_framework.reverse import reverse

from ..settings import (
    AM_ABS_DIRECTORY,
    AM_REL_DIRECTORY,
    AM_URL,
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
)
from .tasks import create_step, process, validate


class UserViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows users to be viewed or edited.
    """

    queryset = User.objects.all().order_by("-date_joined")
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]

    @action(detail=True, url_name="user-archives")
    def archives(self, request, pk=None):
        """
        Gets all the archives for a specific user
        """
        user = self.get_object()
        archives = filter_archives_by_user_perms(user.archives.all(), request.user)
        return self.make_paginated_response(archives, ArchiveSerializer)

    @action(detail=True, url_name="user-archives-staged")
    def archives_staged(self, request, pk=None):
        """
        Gets all the archives for a specific user that are staged
        """
        user = self.get_object()
        archives = filter_archives_by_user_perms(
            user.archives.filter(
                last_step__isnull=True, archive_collections__isnull=True
            ),
            request.user,
        )
        return self.make_paginated_response(archives, ArchiveSerializer)


class GroupViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """

    queryset = Group.objects.all()
    serializer_class = GroupSerializer
    permission_classes = [permissions.IsAuthenticated]


class ArchiveViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows records to be viewed or edited.
    """

    queryset = Archive.objects.all()
    serializer_class = ArchiveSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return filter_archives_by_user_perms(super().get_queryset(), self.request.user)

    @action(detail=True, url_name="archive-steps")
    def archive_steps(self, request, pk=None):
        archive = self.get_object()
        steps = filter_archives_by_user_perms(archive.steps.all(), self.request.user)
        return self.make_paginated_response(archive, ArchiveSerializer)

    @action(detail=True, url_name="get-collections")
    def archive_collections(self, request, pk=None):
        """
        Gets in which collections an archive is
        """
        archive = self.get_object()
        collections = filter_collections_by_user_perms(
            archive.get_collections(), self.request.user
        )
        return self.make_paginated_response(collections, CollectionSerializer)

    @action(detail=True, url_name="search")
    def archive_search(self, request, pk=None):
        """
        Searches if there are other archives of the same source and recid
        """
        archive = self.get_object()
        try:
            archives = Archive.objects.filter(
                recid__contains=archive.recid, source__contains=archive.source
            )
            serializer = ArchiveSerializer(
                filter_archives_by_user_perms(
                    archives,
                    self.request.user,
                ),
                many=True,
            )
            return Response(serializer.data)
        except Archive.DoesNotExist:
            archives = None
            return Response()


class StepViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Step.objects.all().order_by("-start_date")
    serializer_class = StepSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return filter_steps_by_user_perms(super().get_queryset(), self.request.user)

    def approve_or_reject(self, request, permission, approved):
        user = request.user
        if not user.has_perm(permission):
            raise PermissionDenied()

        # Make sure the status of the archive is read and updated atomically,
        # otherwise multiple harvesting task might be scheduled.
        with transaction.atomic():
            step = self.get_object()
            if step.status != Status.WAITING_APPROVAL:
                raise BadRequest("Archive is not waiting for approval")
            if approved:
                step.status = Status.IN_PROGRESS
            else:
                step.status = Status.REJECTED

            step.save()

        if approved:
            if step.name == Steps.HARVEST:
                step.set_status(Status.NOT_RUN)
                process.delay(step.archive.id, step.id)

        serializer = self.get_serializer(step)
        return Response(serializer.data)

    @action(detail=True, methods=["POST"], url_path="actions/approve")
    def approve(self, request, pk=None):
        return self.approve_or_reject(
            request, "oais.can_approve_archive", approved=True
        )

    @action(detail=True, methods=["POST"], url_path="actions/reject")
    def reject(self, request, pk=None):
        return self.approve_or_reject(
            request, "oais.can_reject_archive", approved=False
        )


class CollectionViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows collections to be viewed or edited.
    """

    queryset = Collection.objects.all()
    serializer_class = CollectionSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return Collection.objects.all()

    @action(detail=True, url_name="collection-archives")
    def collection_archives(self, request, pk=None):
        collection = self.get_object()
        archives = collection.archives.all()
        return self.make_paginated_response(collection, CollectionSerializer)

    def add_or_remove(self, request, permission, add):
        user = request.user
        # if not user.has_perm(permission):
        #     raise PermissionDenied()

        if request.data["archives"] == None:
            raise Exception("No archives selected")
        else:
            archives = request.data["archives"]

        with transaction.atomic():
            collection = self.get_object()

        if add:
            for archive in archives:
                if type(archive) == int:
                    collection.add_archive(archive)
                else:
                    collection.add_archive(archive["id"])

        else:
            for archive in archives:
                if type(archive) == int:
                    collection.remove_archive(archive)
                else:
                    collection.remove_archive(archive["id"])

        collection.set_modification_timestamp()
        collection.save()
        serializer = self.get_serializer(collection)
        return Response(serializer.data)

    def delete_collection(self, request, permission):
        user = request.user
        # if not user.has_perm(permission):
        #     raise PermissionDenied()

        with transaction.atomic():
            collection = self.get_object()

        collection.delete()

        return Response()

    @action(detail=True, methods=["POST"], url_path="actions/add")
    def add(self, request, pk=None):
        return self.add_or_remove(request, "oais.can_approve_archive", add=True)

    @action(detail=True, methods=["POST"], url_path="actions/remove")
    def remove(self, request, pk=None):
        return self.add_or_remove(request, "oais.can_reject_archive", add=False)

    @action(
        detail=True,
        methods=["POST"],
        url_path="actions/delete",
        url_name="collections-delete",
    )
    def delete(self, request, pk=None):
        return self.delete_collection(request, "oais.can_reject_archive")


@api_view(["GET"])
def get_settings(request):
    try:
        githash = (
            subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
            .decode("ascii")
            .strip()
        )
    except Exception:
        githash = "n/a"
    data = {
        "am_url": AM_URL,
        "AM_ABS_DIRECTORY": AM_ABS_DIRECTORY,
        "AM_REL_DIRECTORY": AM_REL_DIRECTORY,
        "git_hash": githash,
        "CELERY_BROKER_URL": CELERY_BROKER_URL,
        "CELERY_RESULT_BACKEND": CELERY_RESULT_BACKEND,
    }
    return Response(data)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def get_steps(request, id):
    # Getting jobs for the provided archive ID
    archive = Archive.objects.get(pk=id)
    steps = archive.steps.all().order_by("start_date")

    serializer = StepSerializer(steps, many=True)
    return Response(serializer.data)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def archive_details(self, id):
    archive = Archive.objects.get(pk=id)
    serializer = ArchiveSerializer(archive, many=False)
    return Response(serializer.data)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def collection_details(self, id):
    collection = Collection.objects.get(pk=id)
    serializer = CollectionSerializer(collection, many=False)
    return Response(serializer.data)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def create_collection(request):
    title = request.data["title"]
    description = request.data["description"]
    archives = request.data["archives"]

    collection = Collection.objects.create(
        title=title,
        description=description,
        creator=request.user,
    )
    if archives:
        collection.archives.set(archives)

    serializer = CollectionSerializer(collection, many=False)
    return Response(serializer.data)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def create_next_step(request):

    next_step = request.data["next_step"]
    archive = request.data["archive"]

    if int(next_step) in Steps:
        next_step = create_step(next_step, archive["id"], archive["last_step"])
    else:
        raise Exception("Wrong Step input")

    serializer = StepSerializer(next_step, many=False)
    return Response(serializer.data)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def harvest(request, recid, source):
    try:
        url = get_source(source).get_record_url(recid)
    except InvalidSource:
        raise BadRequest("Invalid source")

    # Always create a new archive instance
    archive = Archive.objects.create(
        recid=recid,
        source=source,
        source_url=url,
        creator=request.user,
    )

    step = Step.objects.create(
        archive=archive, name=Steps.HARVEST, status=Status.WAITING_APPROVAL
    )

    return redirect(
        reverse("archive-detail", request=request, kwargs={"pk": archive.id})
    )


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def upload(request):
    file = request.FILES.getlist("file")[0]

    # WORKAROUND FOR NOW : Get directory name from compressed filename
    # TODO getting source and recid from sip.json?
    try:
        sip_dir = file.name.split(".")[0]
        sip_data = sip_dir.split("::")
        source = sip_data[1]
        recid = sip_data[2]
    except:
        raise BadRequest("Wrong file format")

    try:
        url = get_source(source).get_record_url(recid)
    except InvalidSource:
        raise BadRequest("Invalid source")

    # Always create a new Archive instance
    archive = Archive.objects.create(
        recid=recid,
        source=source,
        defaults={"source_url": url},
        creator=request.user,
    )

    step = Step.objects.create(
        archive=archive, name=Steps.SIP_UPLOAD, status=Status.IN_PROGRESS
    )

    archive.set_step(step)

    # Using root tmp folder
    base_path = os.path.join(os.getcwd(), "tmp")
    try:
        # Save compressed SIP
        compressed_path = os.path.join(base_path, "compressed.zip")
        destination = open(compressed_path, "wb+")
        for chunk in file.chunks():
            destination.write(chunk)
        destination.close()

        # Extract it
        with zipfile.ZipFile(compressed_path, "r") as compressed:
            compressed.extractall(base_path)

        # Remove zip
        os.remove(compressed_path)

        # Uploading completed
        step.set_status(Status.COMPLETED)
        step.set_finish_date()

        # Save path and change status of the archive
        archive.path_to_sip = os.path.join(base_path, sip_dir)
        archive.save()

        next_step = Step.objects.create(
            archive=archive,
            name=Steps.VALIDATION,
            input_step=step.id,
            status=Status.WAITING_APPROVAL,
        )
    except Exception:
        step.set_status(Status.FAILED)

    return Response({"msg": "SIP uploading started, see Archives page"})


@api_view()
@permission_classes([permissions.IsAuthenticated])
def search(request, source):
    if "q" not in request.GET:
        raise BadRequest("Missing parameter q")
    query = request.GET["q"]

    if "p" not in request.GET:
        page = 1
    else:
        page = request.GET["p"]

    if "s" not in request.GET:
        size = 20
    else:
        size = request.GET["s"]

    try:
        results = get_source(source).search(query, page, size)
    except InvalidSource:
        raise BadRequest("Invalid source")

    return Response(results)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def search_by_id(request, source, recid):
    try:
        result = get_source(source).search_by_id(recid.strip())
    except InvalidSource:
        raise BadRequest("Invalid source")

    return Response(result)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def search_query(request):
    """
    Gets the API request from the ReactSearchkit component and returns
    the results based on the elasticsearch response
    """
    # Starts time calculation of the operation
    start_time = time.time()

    """
    Creates a list of dictionaries for the BucketAggregation component

    More info:
    https://inveniosoftware.github.io/react-searchkit/docs/filters-aggregations
    """
    sources = ["indico", "cds", "inveniordm", "zenodo", "cod", "cds-test", "local"]
    buckets = list()
    for source in sources:
        buckets.append({"key": source, "doc_count": 0})

    """
    Gets the API request data based and parses it according to
    the elasticsearch API request

    {"query":{
        "query_string":{
            "query":"example_query"
            }
        },
        "size":10,
        "from":0,
        "aggs":{
            "tags_agg":{
                "terms":{
                    "field":"tags"
    }}}}}
    """
    body = request.data
    if "query" not in body:
        raise BadRequest("Missing parameter query")
    query = body["query"]

    if "query_string" not in query:
        raise BadRequest("Missing parameter query_string")
    query_string = query["query_string"]

    if "query" not in query_string:
        raise BadRequest("Missing parameter search_query")
    search_query = query_string["query"]

    try:
        # Make the search at the database
        results = Archive.objects.filter(recid__contains=search_query)

        serializer = ArchiveSerializer(results, many=True)
    except Exception:
        raise BadRequest("Error while performing search")

    try:
        """
        Create response similar to Elasticsearch response:

        {
            "took": TIME ELAPSED,
            "timed_out" : false,
            "hits" : {
                "total":{
                    "value" : NUMBER OF RESULTS
                    "relation" : "eq"
                },
                "max_score" : ELASTIC SEARCH MAX SCORE GIVEN,
                "hits" : [ARCHIVE LIST OF RESULTS]
            }
            "aggregations":{
                "first_agg": {
                    "doc_count_error_upper_bound":0,
                    "sum_other_doc_count":0,
                    "buckets": [BUCKET LIST]
                }

            }
        }
        """
        response = dict()
        hits = dict()
        aggDetails = dict()

        response["took"] = time.time() - start_time
        response["timeout"] = False
        hits["total"] = {"value": len(results), "relation": "eq"}
        hits["max_score"] = 5
        result_list = []
        for i in range(len(results)):
            hitsDetails = dict()
            hitsDetails["_index"] = "random"
            hitsDetails["_type"] = "doc"
            hitsDetails["_id"] = "CustomID"
            hitsDetails["_score"] = 5
            result = serializer.data[i]
            hitsDetails["_source"] = result
            if result["source"] in sources:
                current_src = result["source"]
                for source in buckets:
                    if source["key"] == current_src:
                        source["doc_count"] = source["doc_count"] + 1

            result_list.append(hitsDetails)

        # Here we need to parse filters based on request
        aggDetails["source_agg"] = {
            "doc_count_error_upper_bound": 0,
            "sum_other_doc_count": 0,
            "buckets": buckets,
        }

        hits["hits"] = result_list
        response["aggregations"] = aggDetails
        response["hits"] = hits

    except Exception:
        raise BadRequest("Error while creating response")

    return Response(response)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def me(request):
    serializer = UserSerializer(request.user)
    return Response(serializer.data)


@api_view(["POST"])
def get_detailed_archives(request):
    """
    Given a list of Archives, returns more information like collection and duplicates
    """
    archives = request.data["archives"]
    for archive in archives:
        id = archive["id"]
        current_archive = Archive.objects.get(pk=id)
        serialized_archive_collections = filter_collections_by_user_perms(
            current_archive.get_collections(), request.user
        )
        serialized_collections = CollectionSerializer(
            serialized_archive_collections, many=True
        )
        archive["collections"] = serialized_collections.data
        try:
            duplicates = Archive.objects.filter(
                recid__contains=archive["recid"], source__contains=archive["source"]
            ).exclude(id__contains=archive["id"])
            archive_serializer = ArchiveSerializer(
                filter_archives_by_user_perms(
                    duplicates,
                    request.user,
                ),
                many=True,
            )
            archive["duplicates"] = archive_serializer.data
        except Archive.DoesNotExist:
            archive["duplicates"] = None

    return Response(archives)


@api_view(["POST"])
def save_manifest(request, id):
    """
    Update the manifest for the specified Archive with the given content
    """
    archive = Archive.objects.get(pk=id)

    step = Step.objects.create(
        archive=archive,
        name=Steps.EDIT_MANIFEST,
        input_step=archive.last_step,
        # change to waiting/not run
        status=Status.IN_PROGRESS,
        input_data=archive.manifest,
    )

    try:
        body = request.data
        if "manifest" not in body:
            raise BadRequest("Missing manifest")
        manifest = body["manifest"]
        archive.set_archive_manifest(manifest)
        step.set_output_data(manifest)
        step.set_status(Status.COMPLETED)
        step.set_finish_date()
        return Response()
    except Exception:
        step.set_status(Status.FAILED)
        step.set_finish_date()
        raise BadRequest("An error occured while saving the manifests.")


@api_view(["POST"])
def login(request):
    serializer = LoginSerializer(data=request.data)
    if serializer.is_valid():
        username = serializer.data["username"]
        password = serializer.data["password"]

        user = auth.authenticate(username=username, password=password)
        if user is not None:
            auth.login(request, user)
            return redirect(reverse("me", request=request))
        else:
            raise BadRequest("Cannot authenticate user")

    raise BadRequest("Missing username or password")


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def logout(request):
    auth.logout(request)
    return Response({"status": "success"})
