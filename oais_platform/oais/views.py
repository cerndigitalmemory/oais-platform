import json
import os
import shutil
import subprocess
import tempfile
import time
import zipfile
from pathlib import PurePosixPath
from shutil import make_archive
from urllib.parse import unquote, urlparse
from wsgiref.util import FileWrapper

from bagit_create import main as bic
from django.conf import settings
from django.contrib import auth
from django.contrib.auth.models import Group, User
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import get_object_or_404, redirect
from drf_spectacular.utils import extend_schema, extend_schema_view
from oais_platform.oais.exceptions import BadRequest
from oais_platform.oais.mixins import PaginationMixin
from oais_platform.oais.models import (
    Archive,
    Collection,
    Profile,
    Status,
    Step,
    Steps,
    UploadJob,
)
from oais_platform.oais.permissions import (
    filter_all_archives_user_has_access,
    filter_archives_by_user_creator,
    filter_archives_for_user,
    filter_archives_public,
    filter_collections_by_user_perms,
    filter_jobs_by_user_perms,
    filter_steps_by_user_perms,
    has_user_archive_edit_rights,
)
from oais_platform.oais.serializers import (
    ArchiveSerializer,
    CollectionSerializer,
    GroupSerializer,
    LoginSerializer,
    ProfileSerializer,
    SourceRecordSerializer,
    StepSerializer,
    UserSerializer,
)
from oais_platform.oais.sources import InvalidSource, get_source
from oais_utils.validate import get_manifest
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
    INVENIO_API_TOKEN,
    INVENIO_SERVER_URL,
)
from .tasks import create_step, process, run_next_step, announce_sip


class UserViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows Users to be viewed or edited
    """

    queryset = User.objects.all().order_by("-date_joined")
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]

    @action(detail=True, url_path="archives", url_name="archives")
    def archives(self, request, pk=None):
        """
        Returns all Archives of a User
        """
        user = self.get_object()
        archives = filter_all_archives_user_has_access(
            # user.archives.all() returns every Archive the User has created
            user.archives.all(),
            request.user,
        )
        return self.make_paginated_response(archives, ArchiveSerializer)

    @action(detail=False, methods=["GET", "POST"], url_path="me", url_name="me")
    def get_set_me(self, request):
        """
        Returns information and settings about the User or,
        updates its profile using the passed values to overwrite
        """
        if request.method == "POST":
            user = request.user

            serializer = ProfileSerializer(data=request.data)
            if serializer.is_valid():
                user.profile.update(serializer.data)
                user.save()

            # TODO: compare the serialized values to comunicate back if some values where ignored/what was actually taken into consideration
            # if (serializer.data == request.data)

            serializer = self.get_serializer(user)
            return Response(serializer.data)

        elif request.method == "GET":
            user = request.user
            serializer = self.get_serializer(user)
            return Response(serializer.data)

    @action(detail=False, url_path="me/tags", url_name="me-tags")
    def get_tags(self, request):
        """
        Returns all Tags created by the User
        """
        try:
            user = request.user
        except InvalidSource:
            raise BadRequest("Invalid request")

        tags = Collection.objects.filter(creator=user, internal=False)
        serializer = CollectionSerializer(tags, many=True)
        return Response(serializer.data)

    @action(detail=False, url_path="me/stats", url_name="me-stats")
    def get_steps_status(self, request):
        """
        Returns all Steps and status of the User
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

    @action(detail=False, url_path="me/sources", url_name="me-sources")
    def get_source_status(self, request):
        """
        Exposes the configuration status of the various upstream sources supported by
        the platform.
        """

        profile = Profile.objects.get(user=request.user)

        indico_api_key = profile.indico_api_key
        codimd_api_key = profile.codimd_api_key
        sso_comp_token = profile.sso_comp_token

        data = {}

        # Ready means that the source is configure for both private and public records
        READY = 1
        # The source works, but for it to return private results to it needs additional configuration
        NEEDS_CONFIG_PRIVATE = 2
        # The source is lacking mandatory configuration values and it won't work in this state
        NEEDS_CONFIG = 3

        data = {
            "zenodo": {"status": READY, "name": "Zenodo"},
            "indico": {"name": "Indico"},
            "codimd": {"name": "CodiMD"},
            "cds": {"name": "CERN Document Server"},
        }

        data["zenodo"]["status"] = READY

        if indico_api_key:
            data["indico"]["status"] = READY
        else:
            data["indico"]["status"] = NEEDS_CONFIG_PRIVATE
        if codimd_api_key:
            data["codimd"]["status"] = READY
        else:
            data["codimd"]["status"] = NEEDS_CONFIG
        if sso_comp_token:
            data["cds"]["status"] = READY
        else:
            data["cds"]["status"] = NEEDS_CONFIG_PRIVATE

        # TODO: Additional checks can be added here to verify the functioning
        # (e.g. pinging an endpoint to see if it can be authenticated)
        return Response(data)


class GroupViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint that allows Groups to be viewed or edited
    """

    queryset = Group.objects.all()
    serializer_class = GroupSerializer
    permission_classes = [permissions.IsAuthenticated]


class ArchiveViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows Archives to be viewed or edited
    """

    queryset = Archive.objects.all()
    serializer_class = ArchiveSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        """
        Returns Archives based on the visibility filter
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

    @action(detail=True, url_path="details", url_name="sgl-details")
    def archive_details(self, request, pk=None):
        """
        Returns details of an identified Archive
        """
        archives = filter_all_archives_user_has_access(
            super().get_queryset(), request.user
        )
        archive = get_object_or_404(archives, pk=pk)
        serializer = self.get_serializer(archive)
        return Response(serializer.data)

    @action(detail=False, methods=["POST"], url_path="details", url_name="mlt-details")
    def archives_details(self, request):
        """
        Returns details of passed Archives such as Steps, Tags and duplicates
        """
        archives = request.data["archives"]
        for archive in archives:
            id = archive["id"]
            current_archive = Archive.objects.get(pk=id)
            serialized_archive_tags = filter_collections_by_user_perms(
                current_archive.get_collections(), request.user
            )
            serialized_collections = CollectionSerializer(
                serialized_archive_tags, many=True
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

    @action(detail=True, url_path="steps", url_name="steps")
    def archive_steps(self, request, pk=None):
        """
        Returns all Steps of an identified Archive
        """
        archive = self.get_object()
        steps = archive.steps.all().order_by("start_date")

        serializer = StepSerializer(steps, many=True)
        return Response(serializer.data)

    @action(detail=True, url_path="tags", url_name="tags")
    def archive_tags(self, request, pk=None):
        """
        Returns the Tag(s) the Archive has
        """
        archive = self.get_object()
        collections = filter_collections_by_user_perms(
            archive.get_collections(), request.user
        )
        return self.make_paginated_response(collections, CollectionSerializer)

    @action(
        detail=True,
        methods=["POST"],
        url_path="save-manifest",
        url_name="save-manifest",
    )
    def archive_save_manifest(self, request, pk=None):
        """
        Update the manifest for the specified Archive with the given content
        """
        archive = Archive.objects.get(pk=pk)

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

    @action(detail=True, url_path="search", url_name="search")
    def archives_search(self, request, pk=None):
        """
        Returns similar Archives (same Source and Recid) if any, nothing otherwise
        """
        archive = self.get_object()
        try:
            archives = Archive.objects.filter(
                recid__contains=archive.recid, source__contains=archive.source
            )
            serializer = ArchiveSerializer(
                filter_archives_by_user_creator(
                    archives,
                    request.user,
                ),
                many=True,
            )
            return Response(serializer.data)
        except Archive.DoesNotExist:
            archives = None
            return Response()

    @extend_schema(operation_id="sgl-unstage")
    @action(detail=True, methods=["POST"], url_path="unstage", url_name="sgl-unstage")
    def archive_unstage(self, request, pk=None):
        """
        Unstages the passed Archive, setting them to the Harvest stage
        """

        # If the user has 'can_unstage' permission and it's not a superuser, return Unauthorized
        if not (request.user.has_perm("oais.can_unstage") or request.user.is_superuser):
            raise BadRequest("Unauthorized")

        archive = self.get_object()
        archive.set_unstaged()

        step = Step.objects.create(
            archive=archive, name=Steps.HARVEST, status=Status.NOT_RUN
        )

        process.delay(step.archive.id, step.id)

        serializer = ArchiveSerializer(
            archive,
            many=False,
        )
        return Response(serializer.data)

    @extend_schema(operation_id="mlt-unstage")
    @action(detail=False, methods=["POST"], url_path="unstage", url_name="mlt-unstage")
    def archives_unstage(self, request):
        """
        Unstages the passed Archives, setting them to the Harvest stage
        Archives are also grouped under the same job tag
        """
        archives = request.data["archives"]

        # If the user has 'can_unstage' permission and it's not a superuser, return Unauthorized
        if not (request.user.has_perm("oais.can_unstage") or request.user.is_superuser):
            raise BadRequest("Unauthorized")

        job_tag = Collection.objects.create(
            internal=True,
            creator=request.user,
            title="Internal Job",
        )

        for archive in archives:
            archive = Archive.objects.get(id=archive["id"])
            archive.set_unstaged()
            job_tag.add_archive(archive)

            step = Step.objects.create(
                archive=archive, name=Steps.HARVEST, status=Status.NOT_RUN
            )
            # Step is auto-approved and harvest step runs
            process.delay(step.archive.id, step.id)

        serializer = CollectionSerializer(
            job_tag,
            many=False,
        )
        return Response(serializer.data)

    # no @action to have recid and source variables in the url
    def archive_create(self, request, recid, source):
        """
        Creates an Archive given a Source and a Record ID
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
            reverse("archives-sgl-details", request=request, kwargs={"pk": archive.id})
        )

    @extend_schema(request=SourceRecordSerializer, responses=ArchiveSerializer)
    @action(
        detail=False,
        methods=["POST"],
        url_path="create/harvest",
        url_name="create-harvest",
    )
    def archive_create_by_harvest(self, request):
        """
        Creates an Archive triggering its own harvesting, given the Source and Record ID
        """
        serializer = SourceRecordSerializer(data=request.data)
        if serializer.is_valid():
            source = serializer.data["source"]
            recid = serializer.data["recid"]

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
            reverse("archives-sgl-details", request=request, kwargs={"pk": archive.id})
        )

    @action(detail=True, methods=["POST"], url_path="delete", url_name="delete")
    def archive_delete(self, request, pk=None):
        """
        Deletes the passed Archive
        """
        archive = self.get_object()
        archive.delete()
        return Response()

    @action(detail=True, methods=["POST"], url_path="next-step", url_name="next-step")
    def archive_next_step(self, request, pk=None):
        """
        Creates the next Step of the passed Archive
        """
        next_step = request.data["next_step"]
        archive = request.data["archive"]

        if int(next_step) in Steps:
            if has_user_archive_edit_rights(pk, request.user):
                next_step = create_step(next_step, pk, archive["last_step"])
            else:
                raise Exception("User has no rights to perform a step for this archive")
        else:
            raise Exception("Wrong Step input")

        serializer = StepSerializer(next_step, many=False)
        return Response(serializer.data)

    def get_staging_area(self, request, pk=None):
        """
        Returns all Archives in the staging area of the User
        """
        archives = Archive.objects.filter(staged=True, creator=request.user)
        pagination = request.GET.get("paginated", "true")
        if pagination == "false":
            return Response(ArchiveSerializer(archives, many=True).data)
        else:
            return self.make_paginated_response(archives, ArchiveSerializer)

    def add_to_staging_area(self, request):
        """
        Adds passed Archives to the staging area of the User
        """
        records = request.data["records"]
        try:
            for record in records:
                # Always create a new archive instance
                Archive.objects.create(
                    recid=record["recid"],
                    source=record["source"],
                    source_url=record["source_url"],
                    title=record["title"],
                    creator=request.user,
                    staged=True,
                )
            return Response({"status": 0, "errormsg": None})
        except Exception as e:
            return Response({"status": 1, "errormsg": e})


class StepViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint that allows Steps to be viewed, approved and rejected
    """

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

    @action(detail=True, url_path="download-artifact", url_name="download-artifact")
    def download_artifact(self, request, pk=None):
        step = self.get_object()

        if request.user.id is not step.archive.creator.id:
            return HttpResponse(status=401)

        output_data = json.loads(step.output_data)
        # If this step has an "Artifact" in the output
        if "artifact" in output_data:
            # If this artifact has a path
            # FIXME: It shouldn't be needed to have different behaviours based on the type of the artifact
            if "artifact_localpath" in output_data["artifact"]:
                if output_data["artifact"]["artifact_name"] == "SIP":
                    # FIXME: Workaround, until the artifact creation/schema is decided
                    files_path = output_data["artifact"]["artifact_localpath"]
                    file_name = f"{pk}-sip.zip"
                    path_to_zip = make_archive(files_path, "zip", files_path)
                    response = HttpResponse(
                        FileWrapper(open(path_to_zip, "rb")),
                        content_type="application/zip",
                    )
                    response[
                        "Content-Disposition"
                    ] = 'attachment; filename="{filename}"'.format(filename=file_name)
                    return response
                elif output_data["artifact"]["artifact_name"] == "AIP":
                    # FIXME: Workaround, until the artifact creation/schema is decided
                    files_path = output_data["artifact"]["artifact_path"]
                    file_name = f"{pk}-aip.7z"
                    response = HttpResponse(
                        FileWrapper(open(files_path, "rb")),
                        content_type="application/x-7z-compressed",
                    )
                    response[
                        "Content-Disposition"
                    ] = 'attachment; filename="{filename}"'.format(filename=file_name)
                    return response
        return HttpResponse(status=404)

    @action(detail=True, methods=["POST"], url_path="approve", url_name="approve")
    def approve(self, request, pk=None):
        """
        Approve an identified step
        """
        return self.approve_or_reject(
            request, "oais.can_approve_archive", approved=True
        )

    @action(detail=True, methods=["POST"], url_path="reject", url_name="reject")
    def reject(self, request, pk=None):
        """
        Reject an identified step
        """
        return self.approve_or_reject(
            request, "oais.can_reject_archive", approved=False
        )


class TagViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows Tags to be viewed or edited
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

    @action(detail=False, methods=["POST"], url_path="create", url_name="create")
    def create_tag(self, request):
        """
        Create a Tag with title, description and Archives
        """
        title = request.data["title"]
        description = request.data["description"]
        archives = request.data["archives"]

        is_duplicate = check_for_tag_name_duplicate(title, request.user)

        if is_duplicate:
            raise BadRequest("A tag with the same name already exists!")
        else:
            tag = Collection.objects.create(
                title=title,
                description=description,
                creator=request.user,
                internal=False,
            )
            if archives:
                tag.archives.set(archives)

            serializer = CollectionSerializer(tag, many=False)
            return Response(serializer.data)

    @action(detail=True, methods=["POST"], url_path="edit", url_name="edit")
    def edit_tag(self, request, pk=None):
        """
        Update a Tag with title, description
        """
        title = request.data["title"]
        description = request.data["description"]

        is_duplicate = check_for_tag_name_duplicate(title, request.user)

        if is_duplicate:
            raise BadRequest("A tag with the same name already exists!")
        else:
            with transaction.atomic():
                tag = self.get_object()

            tag.set_title(title)
            tag.set_description(description)
            tag.set_modification_timestamp()

            serializer = CollectionSerializer(tag, many=False)
            return Response(serializer.data)

    @action(detail=True, methods=["POST"], url_path="delete", url_name="delete")
    def delete_tag(self, request, pk=None):
        """
        Delete a Tag
        """
        # This is about deleting tags not archives, create new perm in permissions.py?
        # user = request.user
        # if not user.has_perm("oais.can_reject_archive"):
        #     raise PermissionDenied()

        with transaction.atomic():
            tag = self.get_object()

        tag.delete()
        return Response()

    @action(detail=True, url_path="archives")
    def get_tagged_archives(self, request, pk=None):
        """
        Returns all Archives with a specific Tag
        """
        tag = self.get_object()
        return self.make_paginated_response(tag, CollectionSerializer)

    def add_or_remove_arch(self, request, permission, add):
        # user = request.user
        # if not user.has_perm(permission):
        #     raise PermissionDenied()

        if request.data["archives"] is None:
            raise Exception("No archives selected")
        else:
            archives = request.data["archives"]

        with transaction.atomic():
            tag = self.get_object()

        if add:
            if isinstance(archives, list):
                for archive in archives:
                    tag.add_archive(archive)
            else:
                tag.add_archive(archives)

        else:
            if isinstance(archives, list):
                for archive in archives:
                    if type(archive) == int:
                        tag.remove_archive(archive)
                    else:
                        tag.remove_archive(archive["id"])
            else:
                tag.remove_archive(archives)

        tag.set_modification_timestamp()
        tag.save()
        serializer = self.get_serializer(tag)
        return Response(serializer.data)

    @action(detail=True, methods=["POST"], url_path="add")
    def add_arch(self, request, pk=None):
        """
        Adds identified Tag to the passed Archives
        """
        return self.add_or_remove_arch(request, "oais.can_approve_archive", add=True)

    @action(detail=True, methods=["POST"], url_path="remove")
    def remove_arch(self, request, pk=None):
        """
        Removes identified Tag from the passed Archives
        """
        return self.add_or_remove_arch(request, "oais.can_reject_archive", add=False)


class UploadJobViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint that allows to create UploadJobs, add files, and submit
    """

    queryset = UploadJob.objects.all()
    permission_classes = [permissions.IsAuthenticated]

    @action(detail=False, methods=["POST"], url_path="create", url_name="create-job")
    def create_job(self, request):
        """
        Initializes an UploadJob, returns its id and its corresponding temporary directory.
        """
        # new files will be added to our own tmp dir
        # (the tmp dir handled by Django gets deleted at context exit)
        tmp_dir = tempfile.mkdtemp()

        uj = UploadJob.objects.create(
            creator=request.user, tmp_dir=tmp_dir, files=json.dumps({})
        )
        uj.save()

        return Response({"uploadJobId": uj.id})

    @action(detail=True, methods=["POST"], url_path="add/file", url_name="add-file")
    def add_file(self, request, pk=None):
        """
        Adds the given file to the specified UploadJob. \n
        Reconstructs the original relative path in the UploadJob's corresponding temporary directory.
        """
        # prepare directories preserving the original structure
        uj = self.get_object()
        tmp_dir = uj.tmp_dir
        relative_path, file = request.FILES.items().__iter__().__next__()

        local_path = os.path.join(tmp_dir, os.path.dirname(relative_path))
        if not os.path.exists(local_path):
            os.makedirs(local_path)

        # move newly added file to our own tmp dir
        shutil.move(file.temporary_file_path(), os.path.join(tmp_dir, relative_path))

        uj = self.get_object()
        uj.add_file(os.path.join(tmp_dir, relative_path), relative_path)

        return Response()

    @action(detail=True, methods=["POST"], url_path="sip", url_name="sip")
    def create_sip(self, request, pk=None):
        """
        Creates an SIP calling bagit_create on the specified UploadJob. \n
        Saves the SIP on the env. var BIC_UPLOAD_PATH (current working directory if not declared).
        """
        uj = self.get_object()

        if settings.BIC_UPLOAD_PATH:
            base_path = settings.BIC_UPLOAD_PATH
        else:
            base_path = os.getcwd()

        # Create the SIP with bagit_create
        result = bic.process(
            recid=None,
            source="local",
            loglevel=0,
            target=base_path,
            source_path=uj.tmp_dir,
            author=str(request.user.id),
        )

        if result["status"] != 0:
            raise BadRequest(
                {
                    "status": 1,
                    "msg": "bagit_create failed creating the SIP: "
                    + result["errormsg"],
                }
            )

        # update the db
        sip_name = result["foldername"]
        uj.set_sip_dir(os.path.join(base_path, sip_name))

        return Response({"status": 0, "msg": "SIP created successfully"})

    @action(detail=True, methods=["POST"], url_path="archive", url_name="archvive")
    def create_archive(self, request, pk=None):
        """
        Creates an Archive given the path to an SIP. \n
        Returns id of this Archive if succesful.
        """
        try:
            uj = self.get_object()
            sip_json = get_manifest(uj.sip_dir)
            step = None

            source = sip_json["source"]
            recid = sip_json["recid"]
            url = get_source(source).get_record_url(recid)
            archive = Archive.objects.create(
                recid=recid, source=source, source_url=url, creator=request.user
            )

            step = Step.objects.create(
                archive=archive, name=Steps.SIP_UPLOAD, status=Status.IN_PROGRESS
            )
            archive.set_step(step)

            # Uploading completed
            step.set_status(Status.COMPLETED)
            step.set_finish_date()

            # Save path and change status of the archive
            archive.path_to_sip = uj.sip_dir
            archive.set_archive_manifest(sip_json["audit"])
            archive.update_next_steps(step.name)
            archive.save()
            run_next_step(archive.id, step.id)

            return Response(
                {
                    "status": 0,
                    "archive": archive.id,
                    "msg": "SIP uploaded, see Archives page",
                }
            )

        except TypeError:
            raise BadRequest({"status": 1, "msg": "Check your SIP structure"})
        except Exception as e:
            if step:
                step.set_status(Status.FAILED)
            raise BadRequest({"status": 1, "msg": e})


# called by /settings
@api_view(["GET"])
def get_settings(request):
    """
    Returns a collection of (read-only) the main configuration values and some
    information about the backend
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

    data = {
        "am_url": AM_URL,
        "AM_ABS_DIRECTORY": AM_ABS_DIRECTORY,
        "AM_REL_DIRECTORY": AM_REL_DIRECTORY,
        "git_hash": githash,
        "CELERY_BROKER_URL": CELERY_BROKER_URL,
        "CELERY_RESULT_BACKEND": CELERY_RESULT_BACKEND,
        "INVENIO_SERVER_URL": INVENIO_SERVER_URL,
        "INVENIO_API_TOKEN": INVENIO_API_TOKEN,
    }

    return Response(data)


@api_view(["GET"])
@permission_classes([permissions.IsAuthenticated])
def statistics(request):
    data = {
        "archives": Archive.objects.count(),
        "harvest": Step.objects.filter(name=2).count(),
        "announce": Step.objects.filter(name=8).count(),
    }
    return Response(data)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def check_archived_records(request):
    """
    Gets a list of records and searches the database for similar archives (same recid + source)
    Then returns the list of records with an archive list field which containes the similar archives
    """
    records = request.data["recordList"]

    if records is None:
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
def harvest(request, id):
    """
    Creates an Archive given the Source and Recid and assigns a havert Step to it
    """
    archive = Archive.objects.get(pk=id)

    Step.objects.create(
        archive=archive, name=Steps.HARVEST, status=Status.WAITING_APPROVAL
    )

    return redirect(
        reverse("archives-sgl-details", request=request, kwargs={"pk": archive.id})
    )


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


@api_view()
@permission_classes([permissions.IsAuthenticated])
def get_archive_information_labels(request):
    """
    Get archive
    """
    try:
        user = request.user
    except InvalidSource:
        raise BadRequest("Invalid request")

    staged_archives = Archive.objects.filter(staged=True, creator=user)
    unstaged_archives = Archive.objects.filter(staged=False, creator=user)

    return Response(
        {"staged": len(staged_archives), "unstaged": len(unstaged_archives)}
    )


@extend_schema_view(
    post=extend_schema(
        description="""Creates an Archive given an UploadedFile
        representing a zipped SIP"""
    )
)
@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def upload_sip(request):
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
            top = [item.split("/")[0] for item in compressed.namelist()]
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

        return Response(
            {
                "status": 0,
                "archive": archive.id,
                "msg": "SIP uploaded, see Archives page",
            }
        )
    except zipfile.BadZipFile:
        raise BadRequest({"status": 1, "msg": "Check the zip file for errors"})
    except TypeError:
        if os.path.exists(compressed_path):
            os.remove(compressed_path)
        raise BadRequest({"status": 1, "msg": "Check your SIP structure"})
    except Exception as e:
        if os.path.exists(compressed_path):
            os.remove(compressed_path)
        if step:
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
        if source == "indico":
            api_token = request.user.profile.indico_api_key
        elif source == "codimd":
            api_token = request.user.profile.codimd_api_key
        else:
            api_token = None
        results = get_source(source, api_token).search(query, page, size)
    except InvalidSource:
        raise BadRequest("Invalid source")

    return Response(results)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def search_by_id(request, source, recid):
    try:
        if source == "indico":
            api_token = request.user.profile.indico_api_key
        elif source == "codimd":
            api_token = request.user.profile.codimd_api_key
        else:
            api_token = None
        result = get_source(source, api_token).search_by_id(recid.strip())
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
            if (source_filter is None) and (visibility_filter is None):
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
            return redirect(reverse("users-me", request=request))
        else:
            raise BadRequest("Cannot authenticate user")

    raise BadRequest("Missing username or password")


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def logout(request):
    auth.logout(request)
    return Response({"status": "success"})


@permission_classes([permissions.IsAuthenticated])
def check_for_tag_name_duplicate(title, creator):
    """
    Given the tag title and the creator checks if there is another tag with the same name
    created by the same person.
    """
    try:
        Collection.objects.get(title=title, creator=creator)
        return True
    except Collection.DoesNotExist:
        return False


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def announce(request):
    """
    Announce the path of SIP to import it into the system.
    The SIP will be validated, copied to the platform designated storage and
    an Archive will be created
    """

    # Get the path passed in the request
    announce_path = request.data["announce_path"]

    # Check if the path is allowed (a user is only allowed to "announce" paths in their home folder on EOS)
    if (
        # Superusers are allowed to announce any path
        request.user.is_superuser is False
        and check_allowed_path(announce_path, request.user.username) is False
    ):
        raise BadRequest("You're not allowed to announce this path")

    # Run the "announce" procedure (validate, copy, create an Archive)
    announce_response = announce_sip(announce_path, request.user)

    # If the process was successful, redirect to the detail of the newly created Archive
    if announce_response["status"] == 0:
        return redirect(
            reverse(
                "archives-sgl-details",
                request=request,
                kwargs={"pk": announce_response["archive_id"]},
            )
        )
    # otherwise, return why the announce failed
    else:
        raise BadRequest(announce_response["errormsg"])


def check_allowed_path(path, username):
    allowed_starting_path = f"/eos/home-{username[0]}/{username}/"
    if path.startswith(allowed_starting_path):
        return True
    else:
        return False
