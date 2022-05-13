import os
import subprocess
import time
from urllib.error import HTTPError
import zipfile
from pathlib import PurePosixPath
from urllib.parse import unquote, urlparse
from django.conf import settings 
from django.contrib import auth
from django.contrib.auth.models import Group, User
from django.db import transaction
from django.db.models import Q
from django.shortcuts import redirect
from drf_spectacular.utils import extend_schema, extend_schema_view
from oais_platform.oais.exceptions import BadRequest, DoesNotExist
from oais_platform.oais.mixins import PaginationMixin
from oais_platform.oais.models import Archive, Collection, Status, Step, Steps
from oais_platform.oais.permissions import (
    filter_all_archives_user_has_access,
    filter_archives_by_user_creator,
    filter_archives_for_user,
    filter_archives_public,
    filter_collections_by_user_perms,
    filter_jobs_by_user_perms,
    filter_records_by_user_perms,
    filter_steps_by_user_perms,
)
from oais_platform.oais.serializers import (
    ArchiveSerializer,
    CollectionSerializer,
    GroupSerializer,
    LoginSerializer,
    ProfileSerializer,
    RequestHarvestSerializer,
    StepSerializer,
    UserSerializer,
)
from oais_platform.oais.sources import InvalidSource, get_source
from rest_framework import permissions, viewsets
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework.views import APIView
from oais_platform.settings import BIC_UPLOAD_PATH
from oais_utils.validate import get_manifest

from ..settings import (
    AM_ABS_DIRECTORY,
    AM_REL_DIRECTORY,
    AM_URL,
    CELERY_BROKER_URL,
    CELERY_RESULT_BACKEND,
)
from .tasks import create_step, process, validate, run_next_step


# Get (and set) user data
@extend_schema_view(
    post=extend_schema(
        description="Updates the user profile, overwriting the passed values",
        request=ProfileSerializer,
        responses=UserSerializer,
    ),
    get=extend_schema(
        description="Get complete information and settings (profile) of the user",
        responses=UserSerializer,
    ),
)
@api_view(["POST", "GET"])
@permission_classes([permissions.IsAuthenticated])
def user_get_set(request):
    if request.method == "POST":
        user = request.user

        serializer = ProfileSerializer(data=request.data)
        if serializer.is_valid():
            user.profile.update(serializer.data)
            user.save()

        # TODO: compare the serialized values to comunicate back if some values where ignored/what was actually taken into consideration
        # if (serializer.data == request.data)

        serializer = UserSerializer(user)
        return Response(serializer.data)
    elif request.method == "GET":
        user = request.user
        serializer = UserSerializer(user)
        return Response(serializer.data)


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
        archives = filter_all_archives_user_has_access(
            user.archives.all(), request.user
        )
        return self.make_paginated_response(archives, ArchiveSerializer)

    @action(detail=True, url_name="user-archives_staged")
    def archives_staged(self, request, pk=None):
        """
        Gets all the archives for a specific user that are staged (no steps assigned)
        """
        user = self.get_object()
        archives = filter_all_archives_user_has_access(
            user.archives.filter(
                last_step__isnull=True,
                steps__isnull=True,
                archive_collections__isnull=True,
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
        """
        Gets the results based on the visibility filter
        """
        visibility = self.request.GET.get("filter", "all")

        if visibility == "public":
            return filter_archives_public(super().get_queryset())
        elif visibility == "owned":
            return filter_archives_by_user_creator(
                super().get_queryset(), self.request.user
            )
        elif visibility == "private":
            return filter_archives_for_user(super().get_queryset(), self.request.user)
        else:
            return filter_all_archives_user_has_access(
                super().get_queryset(), self.request.user
            )

    @action(detail=True, url_name="archive-steps")
    def archive_steps(self, request, pk=None):
        archive = self.get_object()
        steps = filter_archives_by_user_creator(archive.steps.all(), self.request.user)
        return self.make_paginated_response(archive, ArchiveSerializer)

    @action(detail=True, url_name="get_collections")
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
                filter_archives_by_user_creator(
                    archives,
                    self.request.user,
                ),
                many=True,
            )
            return Response(serializer.data)
        except Archive.DoesNotExist:
            archives = None
            return Response()

    @action(detail=True, url_name="unstage")
    def archive_unstage(self, request, pk=None):
        archive = self.get_object()
        archive.set_unstaged()

        serializer = ArchiveSerializer(
            archive,
            many=False,
        )
        return Response(serializer.data)

    @action(detail=True, url_name="delete")
    def archive_delete(self, request, pk=None):
        archive = self.get_object()
        archive.delete()

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
        internal = self.request.GET.get("internal")

        if internal == "true":
            return filter_jobs_by_user_perms(super().get_queryset(), self.request.user)
        else:
            return filter_collections_by_user_perms(
                super().get_queryset(), self.request.user
            )

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
            if isinstance(archives, list):
                for archive in archives:
                    collection.add_archive(archive)
            else:
                collection.add_archive(archives)

        else:
            if isinstance(archives, list):
                for archive in archives:
                    if type(archive) == int:
                        collection.remove_archive(archive)
                    else:
                        collection.remove_archive(archive["id"])
            else:
                collection.remove_archive(archives)

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
        url_name="collections_delete",
    )
    def delete(self, request, pk=None):
        return self.delete_collection(request, "oais.can_reject_archive")


@api_view(["GET"])
def get_settings(request):
    """
    Returns a collection of (read-only) the main configuration values and some
    information about the backend
    Also include some user-side settings
    """

    # Try to get the commit hash of the backend
    try:
        githash = (
            subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
            .decode("ascii")
            .strip()
        )
    except Exception:
        githash = "n/a"

    user = request.user
    serializer = UserSerializer(user)

    data = {
        "am_url": AM_URL,
        "AM_ABS_DIRECTORY": AM_ABS_DIRECTORY,
        "AM_REL_DIRECTORY": AM_REL_DIRECTORY,
        "git_hash": githash,
        "CELERY_BROKER_URL": CELERY_BROKER_URL,
        "CELERY_RESULT_BACKEND": CELERY_RESULT_BACKEND,
        "indico_api_key": serializer.data["profile"]["indico_api_key"],
    }

    return Response(data)


@extend_schema(responses=StepSerializer)
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


@extend_schema(responses=CollectionSerializer)
@api_view()
@permission_classes([permissions.IsAuthenticated])
def get_all_tags(request):
    """
    Returns a list of all the Tags a User has created
    """
    try:
        user = request.user
    except InvalidSource:
        raise BadRequest("Invalid request")

    collections = Collection.objects.filter(creator=user, internal=False)
    serializer = CollectionSerializer(collections, many=True)
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
        internal=False,
    )
    if archives:
        collection.archives.set(archives)

    serializer = CollectionSerializer(collection, many=False)
    return Response(serializer.data)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def check_archived_records(request):
    """
    Gets a list of records and searches the database for similar archives (same recid + source)
    Then returns the list of records with an archive list field which containes the similar archives
    """
    records = request.data["recordList"]

    if records == None:
        return Response(None)

    for record in records:
        try:
            archives = Archive.objects.filter(
                recid__contains=record["recid"], source__contains=record["source"]
            )
            serializer = ArchiveSerializer(
                filter_archives_by_user_creator(
                    archives,
                    request.user,
                ),
                many=True,
            )
            record["archives"] = serializer.data
        except Archive.DoesNotExist:
            record["archives"] = None

    return Response(records)


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


@extend_schema(request=RequestHarvestSerializer, responses=ArchiveSerializer)
@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def create_by_harvest(request):
    """
    Creates an Archive triggering an harvest of it, from the specified Source and Record ID.
    """

    serializer = RequestHarvestSerializer(data=request.data)

    if serializer.is_valid():
        source = serializer.data["source"]
        recid = serializer.data["source"]

    try:
        url = get_source(source).get_record_url(recid)
    except InvalidSource:
        raise BadRequest("Invalid source: ", source)

    # Always create a new archive instance
    archive = Archive.objects.create(
        recid=recid,
        source=source,
        source_url=url,
        creator=request.user,
    )

    return redirect(
        reverse("archive-detail", request=request, kwargs={"pk": archive.id})
    )


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def harvest(request, id):
    """
    Gets a source and the recid, creates an archive object and assigns a harvest step on it
    """
    archive = Archive.objects.get(pk=id)

    step = Step.objects.create(
        archive=archive, name=Steps.HARVEST, status=Status.WAITING_APPROVAL
    )

    return redirect(
        reverse("archive-detail", request=request, kwargs={"pk": archive.id})
    )


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def create_archive(request, recid, source):
    """
    Gets a source and the recid and creates an archive object
    """
    try:
        url = get_source(source).get_record_url(recid)
    except InvalidSource:
        raise BadRequest("Invalid source: ", source)

    # Always create a new archive instance
    archive = Archive.objects.create(
        recid=recid,
        source=source,
        source_url=url,
        creator=request.user,
    )

    return redirect(
        reverse("archive-detail", request=request, kwargs={"pk": archive.id})
    )


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def create_staged_archive(request):
    """
    Gets a source and the recid and creates a staged archive object
    """
    record = request.data["record"]

    # Always create a new archive instance
    archive = Archive.objects.create(
        recid=record["recid"],
        source=record["source"],
        source_url=record["source_url"],
        title=record["title"],
        creator=request.user,
        staged=True,
    )

    return redirect(
        reverse("archive-detail", request=request, kwargs={"pk": archive.id})
    )


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def unstage_archives(request):
    """
    Gets an array of archive IDs, unstages them and creates a job tag for all of them
    """

    archives = request.data["archives"]

    job_tag = Collection.objects.create(
        internal=True,
        creator=request.user,
        title="Internal Job",
    )

    for archive in archives:
        archive = Archive.objects.get(id=archive["id"])
        archive.set_unstaged()
        job_tag.add_archive(archive)

        Step.objects.create(
            archive=archive, name=Steps.HARVEST, status=Status.WAITING_APPROVAL
        )

    return Response(archives)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def get_staged_archives(request):
    """
    Get all staged archives
    """
    try:
        user = request.user
    except InvalidSource:
        raise BadRequest("Invalid request")

    archives = Archive.objects.filter(staged=True, creator=user)
    serializer = ArchiveSerializer(archives, many=True)
    return Response(serializer.data)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def upload(request):
    file = request.FILES.getlist("file")[0]
    step = None

    try:
        # Settings must be imported from django.conf.settings in order to be overridable from the tests
        if settings.BIC_UPLOAD_PATH:
            base_path = settings.BIC_UPLOAD_PATH
        else: 
            base_path = os.getcwd()
        # Save compressed SIP
        compressed_path = os.path.join(base_path, f"compressed_{file.name}")
        destination = open(compressed_path, "wb+")
        for chunk in file.chunks():
            destination.write(chunk)
        destination.close()

        # Extract it and get the top directory folder
        with zipfile.ZipFile(compressed_path, "r") as compressed:
            compressed.extractall(base_path)
            top = [item.split('/')[0] for item in compressed.namelist()]
        os.remove(compressed_path)
    
        # Get the folder location and the sip_json using oais utils
        folder_location = top[0]     
        sip_json = get_manifest(os.path.join(base_path, folder_location))
        sip_location = os.path.join(base_path, folder_location)

        source = sip_json["source"]
        recid = sip_json["recid"]

        url = get_source(source).get_record_url(recid)

        
        # Create a new Archive instance
        archive = Archive.objects.create(
            recid=recid,
            source=source,
            source_url=url,
            creator=request.user,
        )

        step = Step.objects.create(
            archive=archive, name=Steps.SIP_UPLOAD, status=Status.IN_PROGRESS
        )

        archive.set_step(step)   

        # Uploading completed
        step.set_status(Status.COMPLETED)
        step.set_finish_date()

        # Save path and change status of the archive
        archive.path_to_sip = sip_location
        archive.update_next_steps(step.name)
        archive.save()

        run_next_step(archive.id, step.id)

        return Response({"status": 0,"archive":archive.id,"msg": "SIP uploading started, see Archives page"})
    except zipfile.BadZipFile:
        raise BadRequest({"status": 1, "msg":"Check the zip file for errors"})
    except TypeError:
        if(os.path.exists(compressed_path)):
            os.remove(compressed_path)
        raise BadRequest({"status": 1, "msg":"Check your SIP structure"})
    except Exception as e:
        if(os.path.exists(compressed_path)):
            os.remove(compressed_path)
        if(step):
            step.set_status(Status.FAILED)
        raise BadRequest({"status": 1, "msg": e})

    


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
# @permission_classes([permissions.IsAuthenticated])
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
    visibilities = ["private", "public", "owned"]
    source_buckets = list()
    visibility_buckets = list()
    for source in sources:
        source_buckets.append({"key": source, "doc_count": 0})
    for visibility in visibilities:
        visibility_buckets.append({"key": visibility, "doc_count": 0})

    """
    Get request body
    """
    body = request.data

    """
    Get pagination parameters
    """
    results_from, results_size = 0, 20
    if "from" in body:
        results_from = body["from"]

    if "size" in body:
        results_size = body["size"]

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

    if "query" not in body:
        search_query = ""
    else:
        """
        If there is no query in body, return all the results
        """
        query = body["query"]

        if "query_string" not in query:
            raise BadRequest("Missing parameter query_string")
        query_string = query["query_string"]

        if "query" not in query_string:
            raise BadRequest("Missing parameter search_query")
        search_query = query_string["query"]

    post_filter = None
    if "post_filter" not in body:
        """
        Post filter indicates that there are not active filters in the search request.
        In that case a search in the database is executed without further filtering
        """
        try:
            results = Archive.objects.filter(
                Q(recid__contains=search_query)
                | Q(title__contains=search_query)
                | Q(id__contains=search_query)
            )
            unfiltered_results = results

            unfiltered_serializer = ArchiveSerializer(unfiltered_results, many=True)
            filtered_serializer = ArchiveSerializer(results, many=True)
        except Exception:
            raise BadRequest("Error while performing search")
    else:
        post_filter = body["post_filter"]
        if "bool" not in post_filter:
            raise BadRequest("Parameter Error: bool is not in body")
        post_filter = post_filter["bool"]
        if "must" not in post_filter:
            raise BadRequest("Parameter Error: must is not in body")
        post_filter = post_filter["must"]
        source_filter, visibility_filter = None, None
        for filter in post_filter:
            if "terms" not in filter:
                raise BadRequest("Parameter Error: terms is not in body")
            filter = filter["terms"]
            if "source" in filter:
                source_filter = filter["source"]
            if "visibility" in filter:
                visibility_filter = filter["visibility"]
            if (source_filter == None) and (visibility_filter == None):
                raise BadRequest("Parameter Error: Filtering parameter is not in body")

        # try:
        # Make the search at the database
        # If there is no visibility selected then return all public, private and owned records
        unfiltered_results = filter_all_archives_user_has_access(
            Archive.objects.filter(
                Q(recid__contains=search_query)
                | Q(title__contains=search_query)
                | Q(id__contains=search_query)
            ),
            request.user,
        )
        results = unfiltered_results
        if visibility_filter:
            if visibility_filter[0] == "public":
                results = filter_archives_public(
                    Archive.objects.filter(
                        Q(recid__contains=search_query)
                        | Q(title__contains=search_query)
                    )
                )
            elif visibility_filter[0] == "private":
                results = filter_archives_for_user(
                    Archive.objects.filter(
                        Q(recid__contains=search_query)
                        | Q(title__contains=search_query)
                    ),
                    request.user,
                )
            elif visibility_filter[0] == "owned":
                results = filter_archives_by_user_creator(
                    Archive.objects.filter(
                        Q(recid__contains=search_query)
                        | Q(title__contains=search_query)
                    ),
                    request.user,
                )

        if source_filter:
            results = results.filter(source__in=source_filter)

        unfiltered_serializer = ArchiveSerializer(unfiltered_results, many=True)
        filtered_serializer = ArchiveSerializer(results, many=True)

        # except Exception:
        #     raise BadRequest("Error while performing search")

    # try:
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
    """
    Create pagination by returning different results according to the results_from and results_size variables.
    If the results_size is bigger than the results index length, then it is changed to match the exact length
    """
    if results_from + results_size > len(results):
        results_size = len(results) - results_from

    for i in range(results_from, results_from + results_size):
        hitsDetails = dict()
        hitsDetails["_index"] = "random"
        hitsDetails["_type"] = "doc"
        hitsDetails["_id"] = "CustomID"
        hitsDetails["_score"] = 5
        result = filtered_serializer.data[i]
        hitsDetails["_source"] = result

        result_list.append(hitsDetails)

    for j in range(len(unfiltered_results)):
        result = unfiltered_serializer.data[j]
        if result["source"] in sources:
            current_src = result["source"]
            for source in source_buckets:
                if source["key"] == current_src:
                    source["doc_count"] = source["doc_count"] + 1

    for j in range(len(results)):
        result = unfiltered_serializer.data[j]
        public = False
        owned = False
        if not result["restricted"]:
            public = True
        if result["creator"]:
            creator = result["creator"]
            if creator["id"] == request.user.id:
                owned = True
        for visibility in visibility_buckets:
            if visibility["key"] == "public":
                if public:
                    visibility["doc_count"] = visibility["doc_count"] + 1
            if visibility["key"] == "owned":
                if owned:
                    visibility["doc_count"] = visibility["doc_count"] + 1

    # Here we need to parse filters based on request
    aggDetails["sources"] = {
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": 0,
        "buckets": source_buckets,
    }

    aggDetails["visibility_agg"] = {
        "doc_count_error_upper_bound": 0,
        "sum_other_doc_count": 0,
        "buckets": visibility_buckets,
    }

    hits["hits"] = result_list
    response["aggregations"] = aggDetails
    response["hits"] = hits

    # except Exception:
    #     raise BadRequest("Error while creating response")

    return Response(response)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def me(request):
    serializer = UserSerializer(request.user)
    return Response(serializer.data)


@api_view(["POST"])
def get_detailed_archives(request):
    """
    Given a list of Archives, returns more information like steps, collection and duplicates
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

        steps = current_archive.steps.all().order_by("start_date")
        steps_serializer = StepSerializer(steps, many=True)
        archive["steps"] = steps_serializer.data

        try:
            duplicates = Archive.objects.filter(
                recid__contains=archive["recid"], source__contains=archive["source"]
            ).exclude(id__contains=archive["id"])
            archive_serializer = ArchiveSerializer(
                filter_archives_by_user_creator(
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
@permission_classes([permissions.IsAuthenticated])
def get_steps_status(request):
    """
    Gets all the steps for a specific user and status
    """
    try:
        status = request.data["status"]
    except KeyError:
        status = None

    try:
        name = request.data["name"]
    except KeyError:
        name = None

    user = request.user

    if status and name:
        steps = Step.objects.filter(
            status=status, name=name, archive__creator=user
        ).order_by("-start_date")
    elif status:
        steps = Step.objects.filter(status=status, archive__creator=user).order_by(
            "-start_date"
        )
    elif name:
        steps = Step.objects.filter(name=name, archive__creator=user).order_by(
            "-start_date"
        )
    else:
        steps = Step.objects.all().order_by("-start_date")
    filtered_steps = filter_steps_by_user_perms(steps, request.user)
    serializer = StepSerializer(filtered_steps, many=True)
    return Response(serializer.data)


@api_view(["POST"])
def save_manifest(request, id):
    """
    Update the manifest for the specified Archive with the given content
    """
    archive = Archive.objects.get(pk=id)

    try:
        body = request.data
        if "manifest" not in body:
            raise BadRequest("Missing manifest")
        manifest = body["manifest"]

        # If manifest operations are successful, create manifest step
        step = Step.objects.create(
            archive=archive,
            name=Steps.EDIT_MANIFEST,
            input_step=archive.last_step,
            # change to waiting/not run
            status=Status.IN_PROGRESS,
            input_data=archive.manifest,
        )

        archive.set_archive_manifest(manifest)

        step.set_output_data(manifest)
        step.set_status(Status.COMPLETED)
        step.set_finish_date()
        return Response()
    except Exception as e:
        raise BadRequest("An error occured while saving the manifests.", e)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def parse_url(request):

    url = request.data["url"]

    # To be replaced by utils
    o = urlparse(url)
    if o.hostname == "cds.cern.ch":
        source = "cds"
    elif o.hostname == "opendata.cern.ch":
        source = "cod"
    elif o.hostname == "zenodo.org":
        source = "zenodo"
    else:
        raise BadRequest(
            "Unable to parse the given URL. Try manually passing the source and the record ID."
        )

    path_parts = PurePosixPath(unquote(urlparse(url).path)).parts

    # Ensures the path is in the form /record/<RECORD_ID>
    if path_parts[0] == "/" and path_parts[1] == "record":
        # The ID is the second part of the path
        recid = path_parts[2]
    else:
        raise BadRequest(
            "Unable to parse the given URL. Try manually passing the source and the record ID."
        )

    return Response({"recid": recid, "source": source})


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
