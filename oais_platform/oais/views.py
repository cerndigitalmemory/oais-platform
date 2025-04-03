import json
import os
import shutil
import tempfile
import zipfile
from pathlib import PurePosixPath
from shutil import make_archive
from urllib.parse import unquote, urlparse
from wsgiref.util import FileWrapper

from bagit_create import main as bic
from django.conf import settings
from django.contrib import auth
from django.contrib.auth.models import User
from django.db import transaction
from django.db.models import Q
from django.http import HttpResponse
from django.shortcuts import redirect
from drf_spectacular.utils import extend_schema, extend_schema_view
from oais_utils.validate import get_manifest
from rest_framework import permissions, viewsets
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.response import Response
from rest_framework.reverse import reverse
from rest_framework_simplejwt.tokens import RefreshToken

from oais_platform.oais.exceptions import BadRequest
from oais_platform.oais.mixins import PaginationMixin
from oais_platform.oais.models import (
    ApiKey,
    Archive,
    ArchiveState,
    Collection,
    Source,
    Status,
    Step,
    Steps,
    UploadJob,
)
from oais_platform.oais.permissions import (
    ArchivePermission,
    StepPermission,
    SuperUserPermission,
    TagPermission,
    UserPermission,
    filter_archives,
    filter_collections,
)
from oais_platform.oais.serializers import (
    ArchiveSerializer,
    ArchiveWithDuplicatesSerializer,
    CollectionNameSerializer,
    CollectionSerializer,
    LoginSerializer,
    StepSerializer,
    UploadJobSerializer,
    UserSerializer,
)
from oais_platform.oais.sources.utils import InvalidSource, get_source

from ..settings import ALLOW_LOCAL_LOGIN, PIPELINE_SIZE_LIMIT
from . import pipeline
from .tasks import (
    announce_sip,
    batch_announce_task,
    create_step,
    execute_pipeline,
    run_step,
)


class UserViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows Users to be viewed or edited
    """

    queryset = User.objects.all().order_by("-date_joined")
    serializer_class = UserSerializer
    permission_classes = [UserPermission]

    @action(detail=True, url_path="archives", url_name="archives")
    def archives(self, request, pk=None):
        """
        Returns all Archives owned by the User
        """
        user = self.get_object()

        archives = filter_archives(
            Archive.objects.all(),
            user,
            "owned",
        )
        return self.make_paginated_response(archives, ArchiveSerializer)

    @action(detail=False, methods=["GET", "POST"], url_path="me", url_name="me")
    def get_set_me(self, request):
        """
        Returns information and settings about the User or,
        updates its profile using the passed values to overwrite
        """
        user = request.user
        if request.method == "POST":
            source = request.data["source"]
            new_key = request.data["key"]
            try:
                source_obj = Source.objects.get(id=source)
                api_key = ApiKey.objects.get(user=user, source=source_obj)
                if new_key:
                    api_key.key = new_key
                    api_key.save()
                else:
                    api_key.delete()
            except ApiKey.DoesNotExist:
                ApiKey.objects.create(user=user, source=source_obj, key=new_key)
            except Source.DoesNotExist:
                raise BadRequest("Source does not exist")

            # TODO: compare the serialized values to communicate back if some values where ignored/what was actually taken into consideration
            # if (serializer.data == request.data)

            serializer = self.get_serializer(user)
            return Response(serializer.data)

        elif request.method == "GET":
            serializer = self.get_serializer(user)

            # Serializer is immutable, so let's copy it to another dict
            user_data = serializer.data

            # Append the API token, if it exists
            try:
                user_data["api_token"] = str(
                    RefreshToken.for_user(request.user).access_token
                )
            except Exception:
                pass

            sources = Source.objects.filter(enabled=True, has_restricted_records=True)
            user_data["api_key"] = []
            for source in sources:
                entry = {
                    "source_id": source.id,
                    "source": source.longname,
                    "how_to": source.how_to_get_key,
                }
                try:
                    api_key = ApiKey.objects.get(user=user, source=source)
                    entry["key"] = api_key.key
                except ApiKey.DoesNotExist:
                    entry["key"] = None
                user_data["api_key"].append(entry)

            return Response(user_data)

    @action(detail=False, url_path="me/tags", url_name="me-tags")
    def get_tags(self, request):
        """
        Returns all not internal Tags accessible by the User
        """
        user = request.user

        tags = filter_collections(Collection.objects.all(), user, internal=False)
        serializer = CollectionSerializer(tags, many=True)
        return Response(serializer.data)

    @action(
        detail=False,
        methods=["GET"],
        url_path="me/staging-area",
        url_name="me-staging-area",
    )
    def get_staging_area(self, request, pk=None):
        """
        Returns all Archives in the staging area of the User
        """
        user = request.user
        archives = filter_archives(Archive.objects.all(), user)
        staged_archives = archives.filter(staged=True)
        resource_ids = staged_archives.values_list("resource__id", flat=True)

        duplicates = archives.filter(resource__in=resource_ids).exclude(staged=True)

        pagination = request.GET.get("paginated", "true")
        if pagination == "false":
            return Response(
                ArchiveWithDuplicatesSerializer(
                    staged_archives, many=True, context={"duplicates": duplicates}
                ).data
            )
        else:
            return self.make_paginated_response(
                staged_archives,
                ArchiveWithDuplicatesSerializer,
                extra_context={"duplicates": duplicates},
            )

    @action(detail=False, methods=["POST"], url_path="me/stage", url_name="me-stage")
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
                    requester=request.user,
                    staged=True,
                )
            return Response({"status": 0, "errormsg": None})
        except Exception as e:
            return Response({"status": 1, "errormsg": e})

    @action(detail=False, url_path="me/stats", url_name="me-stats")
    def get_steps_status(self, request):
        """
        Returns all Steps for the given name and status of the User
        """
        status = request.data.get("status")
        name = request.data.get("name")

        user = request.user
        archives = filter_archives(
            Archive.objects.all(),
            user,
            "owned",
        )
        step_filter = Q(archive__in=archives)
        if status:
            step_filter |= Q(status=status)
        if name:
            step_filter |= Q(name=name)
        filtered_steps = Step.objects.filter(step_filter).order_by("-start_date")
        serializer = StepSerializer(filtered_steps, many=True)
        return Response(serializer.data)

    @action(detail=False, url_path="me/sources", url_name="me-sources")
    def get_source_status(self, request):
        """
        Exposes the configuration status of the various upstream sources supported by
        the platform.
        """

        # Ready means that the source is configure for both restricted and public records
        READY = 1
        # The source works for public records, but it needs additional configuration for restricted ones
        NEEDS_CONFIG_PRIVATE = 2
        # The source is lacking mandatory configuration values and it won't work in this state
        NEEDS_CONFIG = 3
        # The source configuration is invalid (no public and no restricted records)
        INVALID = 4

        data = {}
        sources = Source.objects.filter(enabled=True)
        for source in sources:
            has_api_key = ApiKey.objects.filter(
                user=request.user, source=source
            ).exists()
            status = READY
            if source.has_restricted_records and not has_api_key:
                if source.has_public_records:
                    status = NEEDS_CONFIG_PRIVATE
                else:
                    status = NEEDS_CONFIG
            elif not source.has_public_records and not source.has_restricted_records:
                status = INVALID
            entry = {
                "id": source.id,
                "name": source.name,
                "longname": source.longname,
                "status": status,
            }
            data[source.name] = entry

        return Response(data)


class ArchiveViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows Archives to be viewed or edited
    """

    queryset = Archive.objects.all()
    serializer_class = ArchiveSerializer
    permission_classes = [ArchivePermission]
    default_page_size = 10
    filters_map = {
        "state": ["state"],
        "source": ["source"],
        "tag": ["archive_collections__id"],
        "step_name": ["last_step__name"],
        "step_status": ["last_step__status"],
        "query": ["title__icontains", "recid__icontains"],
    }

    def get_queryset(self):
        """
        Returns an Archive list based on the given visibility/access filter
        """
        visibility = self.request.GET.get("access", "all")
        page = self.request.GET.get("page", None)
        size = self.request.GET.get("size", self.default_page_size)

        if visibility in ["all", "owned", "public"]:
            result = filter_archives(
                super().get_queryset(), self.request.user, visibility
            )
        else:
            raise BadRequest("Invalid access parameter.")

        if page == "all":
            if not self.request.GET._mutable:
                self.request.GET._mutable = True
            self.request.GET["page"] = 1
            self.pagination_class.page_size = len(result)
        else:
            self.pagination_class.page_size = size

        return result

    @action(detail=False, methods=["POST"], url_path="filter", url_name="filter")
    def archives_filtered(self, request):
        """
        Returns an Archive list based on the filters set
        """
        result = self.get_queryset()

        if "filters" not in request.data:
            raise BadRequest("No filters")

        filters = request.data["filters"]

        try:
            query = Q()
            for key, value in filters.items():
                subquery = Q()
                for query_arg in self.filters_map[key]:
                    subquery |= Q(**{query_arg: value})

                query &= subquery
        except Exception as error:
            match error:
                case KeyError():
                    raise BadRequest("Invalid filter")
                case _:
                    raise BadRequest("Invalid request")

        result = result.filter(query).order_by("-last_modification_timestamp")

        return self.make_paginated_response(result, ArchiveSerializer)

    @action(
        detail=False, methods=["POST"], url_path="duplicates", url_name="duplicates"
    )
    def check_archived_records(self, request):
        """
        Gets a list of records and searches the database for similar archives (same recid + source)
        Then returns the list of records with an archive list field which containes the similar archives
        """
        records = request.data["records"]

        if records is None:
            return Response(None)

        for record in records:
            try:
                duplicates = Archive.objects.filter(
                    recid=record["recid"], source=record["source"]
                ).exclude(state=ArchiveState.NONE)
                serializer = ArchiveSerializer(
                    filter_archives(
                        duplicates,
                        request.user,
                    ),
                    many=True,
                )
                record["archives"] = serializer.data
            except Archive.DoesNotExist:
                record["archives"] = None

        return Response(records)

    @action(detail=False, methods=["GET"], url_path="sources", url_name="sources")
    def archives_sources(self, request):
        """
        Returns all source values from the Archives accessible by the user
        """
        archives = self.get_queryset()
        sources = (
            archives.order_by("source")
            .distinct("source")
            .values_list("source", flat=True)
        )

        return Response(sources)

    @action(detail=True, url_path="steps", url_name="steps")
    def archive_steps(self, request, pk=None):
        """
        Returns all Steps of an identified Archive
        """
        archive = self.get_object()
        steps = archive.steps.all().order_by("start_date", "create_date")

        serializer = StepSerializer(steps, many=True)

        return Response(serializer.data)

    @action(detail=True, url_path="next-steps", url_name="next-steps")
    def archive_next_steps(self, request, pk=None):
        """
        Returns the type of possible next Steps of an identified Archive
        """
        archive = self.get_object()
        next_steps = archive.get_next_steps()

        return Response(next_steps)

    @action(detail=True, url_path="tags", url_name="tags")
    def archive_tags(self, request, pk=None):
        """
        Returns the Tag(s) the Archive has
        """
        archive = self.get_object()
        collections = filter_collections(archive.get_collections(), request.user)
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
        archive = self.get_object()
        try:
            body = request.data
            if "manifest" not in body:
                raise BadRequest("Missing manifest")
            manifest = body["manifest"]

            # If manifest operations are successful, create manifest step
            step = Step.objects.create(
                archive=archive,
                name=Steps.EDIT_MANIFEST,
                input_step=archive.last_completed_step,
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

    @extend_schema(operation_id="sgl-unstage")
    @action(detail=True, methods=["POST"], url_path="unstage", url_name="sgl-unstage")
    def archive_unstage(self, request, pk=None):
        """
        Unstages the passed Archive, setting them to the Harvest stage
        """
        archive = self.get_object()

        archive.set_unstaged(approver=request.user)

        step = Step.objects.create(
            archive=archive, name=Steps.HARVEST, status=Status.NOT_RUN
        )

        try:
            api_key = ApiKey.objects.get(
                source__name=archive.source, user=request.user
            ).key
        except Exception:
            api_key = None

        run_step(step, archive.id, api_key=api_key)

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

        job_tag = Collection.objects.create(
            internal=True,
            creator=request.user,
            title="Internal Job",
        )

        for archive in archives:
            archive = Archive.objects.get(id=archive["id"])
            archive.set_unstaged(approver=request.user)
            job_tag.add_archive(archive)

            step = Step.objects.create(
                archive=archive, name=Steps.HARVEST, status=Status.NOT_RUN
            )
            # Step is auto-approved and harvest step runs
            try:
                api_key = ApiKey.objects.get(
                    source__name=archive.source, user=request.user
                ).key
            except Exception:
                api_key = None
            run_step(step, archive.id, api_key=api_key)

        serializer = CollectionSerializer(
            job_tag,
            many=False,
        )
        return Response(serializer.data)

    @action(
        detail=True,
        methods=["POST"],
        url_path="delete-staged",
        url_name="delete-staged",
    )
    def archive_delete(self, request, pk=None):
        """
        Deletes the staged Archive
        """
        archive = self.get_object()
        if not archive.staged:
            raise BadRequest("Archive must be staged.")
        archive.delete()
        return Response()

    @action(detail=True, methods=["POST"], url_path="pipeline", url_name="pipeline")
    def archive_run_pipeline(self, request, pk=None):
        """
        Creates the pipline of Steps for the passed Archive and executes it
        """
        self.get_object()
        run_type = request.data.get("run_type", "run")
        steps = request.data.get("pipeline_steps")
        archive_id = request.data["archive"]["id"]

        try:
            api_key = ApiKey.objects.get(
                source__name=request.data["archive"]["source"], user=request.user
            ).key
        except Exception:
            api_key = None

        with transaction.atomic():
            archive = Archive.objects.select_for_update().get(pk=archive_id)
            force_continue = False

            match run_type:
                case "run":
                    if steps is not None and (
                        len(steps) > PIPELINE_SIZE_LIMIT or len(steps) == 0
                    ):
                        raise BadRequest("Invalid pipeline size")
                    try:
                        for step_name in steps:
                            archive.add_step_to_pipeline(step_name)
                    except Exception as e:
                        raise BadRequest(e)
                case "retry":
                    force_continue = True
                    last_step = Step.objects.select_for_update().get(
                        pk=archive.last_step.id
                    )
                    if last_step and last_step.status != Status.FAILED:
                        raise BadRequest(
                            "Retry operation not permitted, last step is not failed."
                        )
                    step = create_step(
                        step_name=last_step.name,
                        archive=archive,
                        input_step_id=last_step.id,
                        input_data=last_step.output_data,
                    )

                    # get steps that are preceded by the failed step
                    next_steps = Step.objects.filter(
                        input_step__id=last_step.id
                    ).exclude(id=step.id)

                    # update successors of the failed steps
                    for next_step in next_steps:
                        next_step.set_input_step(step)
                    archive.pipeline_steps.insert(0, step.id)
                    archive.save()
                case "continue":
                    force_continue = True
                    last_step = Step.objects.select_for_update().get(
                        pk=archive.last_step.id
                    )
                    if last_step and last_step.status != Status.FAILED:
                        raise BadRequest(
                            "Continue operation not permitted, last step is not failed."
                        )
                    if len(archive.pipeline_steps) == 0:
                        raise BadRequest(
                            "Continue operation not permitted, the pipeline is empty."
                        )
                    continue_step = Step.objects.select_for_update().get(
                        pk=archive.pipeline_steps[0]
                    )
                    if continue_step.status != Status.WAITING:
                        raise BadRequest(
                            "Continue operation not permitted, next step in pipeline is not in status WAITING."
                        )
                case _:
                    raise BadRequest(
                        "Invalid run_type param, possible values: ('run', 'retry', 'continue')."
                    )

        step = execute_pipeline(
            archive_id, api_key=api_key, force_continue=force_continue
        )
        serializer = StepSerializer(step, many=False)
        return Response(serializer.data)

    @action(detail=False, methods=["POST"], url_path="actions", url_name="actions")
    def archive_action_intersection(self, request, pk=None):
        """
        Get common possible actions for the archives
        """
        archives = request.data["archives"]
        result = {}
        if len(archives) > 0:
            first_state = Archive.objects.get(pk=archives[0]["id"]).state
            state_intersection = True
            next_steps_intersection = None
            all_last_step_failed = True
            can_continue = True

            with transaction.atomic():
                for archive in archives:
                    archive = Archive.objects.select_for_update().get(pk=archive["id"])

                    if state_intersection and archive.state != first_state:
                        state_intersection = False

                    if (
                        not archive.last_step
                        or archive.last_step.status != Status.FAILED
                    ):
                        all_last_step_failed = False

                    if len(archive.pipeline_steps) == 0:
                        can_continue = False

                    next_step = archive.get_next_steps()
                    if not next_steps_intersection:
                        next_steps_intersection = next_step
                    else:
                        next_steps_intersection = set(
                            next_steps_intersection
                        ).intersection(next_step)
                        if (
                            len(next_steps_intersection) == 0
                            and not state_intersection
                            and not all_last_step_failed
                        ):
                            break
                result["state_intersection"] = state_intersection
                result["next_steps_intersection"] = sorted(next_steps_intersection)
                result["all_last_step_failed"] = all_last_step_failed
                result["can_continue"] = all_last_step_failed and can_continue

        return Response(result)


class StepViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint that allows Steps to be viewed, approved and rejected
    """

    queryset = Step.objects.all().order_by("-start_date")
    serializer_class = StepSerializer
    permission_classes = [StepPermission]

    def get_queryset(self):
        user_archives = filter_archives(Archive.objects.all(), self.request.user, "all")
        return Step.objects.filter(archive__in=user_archives).distinct()

    @action(detail=True, url_path="download-artifact", url_name="download-artifact")
    def download_artifact(self, request, pk=None):
        step = self.get_object()

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
                    response["Content-Disposition"] = (
                        'attachment; filename="{filename}"'.format(filename=file_name)
                    )
                    return response
                elif output_data["artifact"]["artifact_name"] == "AIP":
                    # FIXME: Workaround, until the artifact creation/schema is decided
                    files_path = output_data["artifact"]["artifact_path"]
                    file_name = f"{pk}-aip.7z"
                    response = HttpResponse(
                        FileWrapper(open(files_path, "rb")),
                        content_type="application/x-7z-compressed",
                    )
                    response["Content-Disposition"] = (
                        'attachment; filename="{filename}"'.format(filename=file_name)
                    )
                    return response
        return HttpResponse(status=404)

    @action(
        detail=False, methods=["GET"], url_path="constraints", url_name="constraints"
    )
    def get_steps_order_constraints(self, request):
        """
        Returns all Step order constraints
        """
        return Response(pipeline.get_next_steps_constraints())


class TagViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows Tags to be viewed or edited
    """

    queryset = Collection.objects.all()
    serializer_class = CollectionSerializer
    permission_classes = [TagPermission]
    default_page_size = 10

    def get_queryset(self):
        internal = self.request.GET.get("internal")
        size = self.request.GET.get("size", self.default_page_size)
        self.pagination_class.page_size = size
        if internal == "only":
            return filter_collections(
                super().get_queryset(), self.request.user, internal=True
            )
        elif internal == "false":
            return filter_collections(
                super().get_queryset(), self.request.user, internal=False
            )
        else:
            return filter_collections(super().get_queryset(), self.request.user)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = CollectionSerializer(instance)
        return Response(serializer.data)

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
        tag = self.get_object()
        title = request.data["title"]
        description = request.data["description"]

        is_duplicate = check_for_tag_name_duplicate(title, request.user)

        if is_duplicate:
            raise BadRequest("A tag with the same name already exists!")
        else:
            with transaction.atomic():
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
        with transaction.atomic():
            tag = self.get_object()
            tag.delete()
        return Response()

    @action(detail=True, url_path="archives", url_name="archives")
    def get_tagged_archives(self, request, pk=None):
        """
        Returns all Archives with a specific Tag
        """
        tag = self.get_object()
        archives = tag.archives.all()
        return self.make_paginated_response(archives, ArchiveSerializer)

    def add_or_remove_arch(self, request, add):
        if request.data["archives"] is None:
            raise BadRequest("No archives selected")
        else:
            archives = request.data["archives"]

        with transaction.atomic():
            tag = self.get_object()

            if add:
                if isinstance(archives, list):
                    for archive in archives:
                        tag.add_archive(archive)
                else:
                    raise BadRequest("Field 'archives' must be a list.")

            else:
                if isinstance(archives, list):
                    for archive in archives:
                        if type(archive) is int:
                            tag.remove_archive(archive)
                        else:
                            tag.remove_archive(archive["id"])
                else:
                    raise BadRequest("Field 'archives' must be a list.")

            tag.set_modification_timestamp()
            tag.save()
        serializer = self.get_serializer(tag)
        return Response(serializer.data)

    @action(detail=True, methods=["POST"], url_path="add")
    def add_arch(self, request, pk=None):
        """
        Adds identified Tag to the passed Archives
        """
        return self.add_or_remove_arch(request, add=True)

    @action(detail=True, methods=["POST"], url_path="remove")
    def remove_arch(self, request, pk=None):
        """
        Removes identified Tag from the passed Archives
        """
        return self.add_or_remove_arch(request, add=False)

    @action(detail=False, methods=["GET"], url_path="names")
    def get_name_list(self, request, pk=None):
        """
        Returns all Tag names and ids
        """
        tags = self.get_queryset().values("id", "title", "timestamp")
        serializer = CollectionNameSerializer(tags.order_by("-timestamp"), many=True)
        return Response({"result": serializer.data})


class UploadJobViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint that allows to create UploadJobs, add files, and submit
    """

    queryset = UploadJob.objects.all()
    permission_classes = [SuperUserPermission]
    serializer_class = UploadJobSerializer

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

    @action(detail=True, methods=["POST"], url_path="archive", url_name="archive")
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
                recid=recid, source=source, source_url=url, requester=request.user
            )

            step = Step.objects.create(
                archive=archive, name=Steps.SIP_UPLOAD, status=Status.IN_PROGRESS
            )
            step.set_start_date()

            # Uploading completed
            step.set_status(Status.COMPLETED)
            step.set_finish_date()

            # Set Archive's last step info
            archive.set_last_step(step.id)
            archive.set_last_completed_step(step.id)

            # Save path and change status of the archive
            archive.path_to_sip = uj.sip_dir
            archive.set_archive_manifest(sip_json["audit"])
            archive.save()

            # run next step
            execute_pipeline(archive.id)

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


@api_view(["GET"])
def statistics(request):
    harvested_count = Archive.objects.filter(state=ArchiveState.SIP).count()
    preserved_count = Archive.objects.filter(state=ArchiveState.AIP).count()
    data = {
        "harvested_count": harvested_count + preserved_count,
        "preserved_count": preserved_count,
        "pushed_to_tape_count": Step.objects.filter(
            name=Steps.PUSH_TO_CTA, status=Status.COMPLETED
        )
        .values("archive")
        .distinct()
        .count(),
        "pushed_to_registry_count": Step.objects.filter(
            name=Steps.INVENIO_RDM_PUSH, status=Status.COMPLETED
        )
        .values("archive")
        .distinct()
        .count(),
    }
    return Response(data)


@extend_schema_view(
    post=extend_schema(
        description="""Creates an Archive given an UploadedFile
        representing a zipped SIP"""
    )
)
@api_view(["POST"])
@permission_classes([SuperUserPermission])
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
            requester=request.user,
        )

        step = Step.objects.create(
            archive=archive, name=Steps.SIP_UPLOAD, status=Status.IN_PROGRESS
        )
        step.set_start_date()

        # Uploading completed
        step.set_status(Status.COMPLETED)
        step.set_finish_date()

        # Set Archive's last step info
        archive.set_last_step(step.id)
        archive.set_last_completed_step(step.id)

        # Save path and change status of the archive
        archive.path_to_sip = sip_location
        archive.save()

        # run next step
        execute_pipeline(archive.id)

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
        try:
            api_key = ApiKey.objects.get(source__name=source, user=request.user).key
        except Exception:
            api_key = None
        results = get_source(source, api_key).search(query, page, size)
    except InvalidSource:
        raise BadRequest("Invalid source")

    return Response(results)


@api_view()
@permission_classes([permissions.IsAuthenticated])
def search_by_id(request, source, recid):
    try:
        try:
            api_key = ApiKey.objects.get(source__name=source, user=request.user).key
        except Exception:
            api_key = None
        result = get_source(source, api_key).search_by_id(recid.strip())
    except InvalidSource:
        raise BadRequest("Invalid source")

    return Response(result)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def parse_url(request):
    url = request.data["url"]

    # To be replaced by utils
    o = urlparse(url)

    try:
        source = Source.objects.get(api_url__contains=o.hostname).name
    except Exception:
        raise BadRequest(
            "Unable to parse the given URL. Try manually passing the source and the record ID."
        )

    path_parts = PurePosixPath(unquote(urlparse(url).path)).parts
    # Ensures the path is in the form /record/<RECORD_ID>
    if path_parts[0] == "/" and (
        path_parts[1] == "record" or path_parts[1] == "records"
    ):
        # The ID is the second part of the path
        recid = path_parts[2]
    else:
        raise BadRequest(
            "Unable to parse the given URL. Try manually passing the source and the record ID."
        )

    return Response({"recid": recid, "source": source})


@extend_schema(request=LoginSerializer, responses=UserSerializer)
@api_view(["POST"])
def login(request):
    """
    Local accounts login route. If successful, returns the logged in User and Profile.
    """

    if not ALLOW_LOCAL_LOGIN:
        raise BadRequest("Local authentication disabled")

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


@extend_schema(
    request=None,
    # TODO: provide a serializer for 403 here
    responses=None,
)
@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def logout(request):
    """
    Clean out session data for the current request and logs out the active user.
    """
    auth.logout(request)
    return Response({"status": "success"})


@api_view(["POST"])
@permission_classes([SuperUserPermission])
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
                "archives-detail",
                request=request,
                kwargs={"pk": announce_response["archive_id"]},
            )
        )
    # otherwise, return why the announce failed
    else:
        raise BadRequest(announce_response["errormsg"])


@api_view(["POST"])
@permission_classes([SuperUserPermission])
def batch_announce(request):
    """
    Announce the path of folder containing SIP folders to import it into the system.
    The SIPs will be validated, copied to the platform designated storage and
    Archives will be created
    """
    # Get the path passed in the request
    announce_path = request.data["batch_announce_path"]
    batch_tag = request.data["batch_tag"]

    max_title_length = Collection._meta.get_field("title").max_length
    if len(batch_tag) > max_title_length:
        raise BadRequest(f"Tag name length exceeded (limit: {max_title_length})")

    is_duplicate = check_for_tag_name_duplicate(batch_tag, request.user)

    if is_duplicate:
        raise BadRequest("A tag with the same name already exists!")

    # Check if the path is allowed (a user is only allowed to "announce" paths in their home folder on EOS)
    if (
        # Superusers are allowed to announce any path
        request.user.is_superuser is False
        and check_allowed_path(announce_path, request.user.username) is False
    ):
        raise BadRequest("You're not allowed to announce this path")

    try:
        folder_exists = os.path.exists(announce_path)
        if not folder_exists:
            raise BadRequest("Folder does not exist")
        subfolder_count = len(next(os.walk(announce_path))[1])
    except Exception:
        raise BadRequest("Folder does not exist or the oais user has no access")

    subfolder_count_limit = settings.BATCH_ANNOUNCE_LIMIT
    if subfolder_count > subfolder_count_limit:
        raise BadRequest(
            f"Number of subfolder limit exceeded (limit: {subfolder_count_limit})"
        )
    elif subfolder_count < 1:
        raise BadRequest("No subfolders found")

    tag = Collection.objects.create(
        title=batch_tag,
        description="Batch Announce processing...",
        creator=request.user,
        internal=False,
    )

    batch_announce_task.delay(announce_path, tag.id, request.user.id)

    return redirect(reverse("tags-detail", request=None, kwargs={"pk": tag.id}))


@api_view(["GET"])
@permission_classes([permissions.IsAuthenticated])
def sources(request):
    sources = Source.objects.filter(enabled=True).values_list("name", flat=True)
    return Response(sources)


def check_allowed_path(path, username):
    allowed_starting_paths = [
        f"/eos/home-{username[0]}/{username}/",
        f"/eos/user/{username[0]}/{username}/",
    ]
    if path.startswith(tuple(allowed_starting_paths)):
        return True
    else:
        return False


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
