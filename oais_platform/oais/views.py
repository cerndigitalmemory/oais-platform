import logging
import os, zipfile, time
from django.contrib import auth
from django.contrib.auth.models import Group, User
from django.db import transaction
from django.db.models import base
from django.shortcuts import redirect
from oais_platform.oais.exceptions import BadRequest
from oais_platform.oais.mixins import PaginationMixin
from oais_platform.oais.models import Archive, Stages, Status, Record, Job
from oais_platform.oais.permissions import filter_archives_by_user_perms
from oais_platform.oais.serializers import (ArchiveSerializer, GroupSerializer, JobSerializer,
                                            LoginSerializer, RecordSerializer,
                                            UserSerializer)
from oais_platform.oais.sources import InvalidSource, get_source
from rest_framework import permissions, viewsets
from rest_framework.decorators import action, api_view, permission_classes
from rest_framework.exceptions import PermissionDenied
from rest_framework.response import Response
from rest_framework.reverse import reverse

from .tasks import process, validate


class UserViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows users to be viewed or edited.
    """

    queryset = User.objects.all().order_by("-date_joined")
    serializer_class = UserSerializer
    permission_classes = [permissions.IsAuthenticated]

    @action(detail=True, url_name="user-archives")
    def archives(self, request, pk=None):
        user = self.get_object()
        archives = filter_archives_by_user_perms(user.archives.all(), request.user)
        return self.make_paginated_response(archives, ArchiveSerializer)


class GroupViewSet(viewsets.ReadOnlyModelViewSet):
    """
    API endpoint that allows groups to be viewed or edited.
    """

    queryset = Group.objects.all()
    serializer_class = GroupSerializer
    permission_classes = [permissions.IsAuthenticated]


class RecordViewSet(viewsets.ReadOnlyModelViewSet, PaginationMixin):
    """
    API endpoint that allows records to be viewed or edited.
    """

    queryset = Record.objects.all()
    serializer_class = RecordSerializer
    permission_classes = [permissions.IsAuthenticated]

    @action(detail=True, url_name="record-archives")
    def archives(self, request, pk=None):
        record = self.get_object()
        archives = filter_archives_by_user_perms(record.archives.all(), request.user)
        return self.make_paginated_response(archives, ArchiveSerializer)


class ArchiveViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Archive.objects.all().order_by("-creation_date")
    serializer_class = ArchiveSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return filter_archives_by_user_perms(super().get_queryset(), self.request.user)

    def approve_or_reject(self, request, permission, approved):
        user = request.user
        if not user.has_perm(permission):
            raise PermissionDenied()

        # Make sure the status of the archive is read and updated atomically,
        # otherwise multiple harvesting task might be scheduled.
        with transaction.atomic():
            archive = self.get_object()
            if archive.status != Status.WAITING_APPROVAL:
                raise BadRequest("Archive is not waiting for approval")
            if approved:
                archive.status = Status.PENDING
            else:
                archive.status = Status.REJECTED
                job = archive.get_latest_job()
                job.set_rejected()
            archive.save()

        if approved:
            job = archive.get_latest_job()
            if job.stage == Stages.HARVEST_REQUESTED:
                job.set_completed()

                archive.set_pending()
                harvest_job = Job.objects.create(
                    archive = archive,
                    stage = Stages.HARVESTING,
                    status = Status.PENDING
                )

                process.delay(archive.id, harvest_job.id)
            elif job.stage == Stages.VALIDATION_REQUESTED:
                job.set_completed()

                archive.set_pending()
                registry_job = Job.objects.create(
                    archive = archive,
                    stage = Stages.CHECKING_REGISTRY,
                    status = Status.PENDING
                )

                validate.delay(archive.id, archive.path_to_sip, registry_job.id)

        serializer = self.get_serializer(archive)
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

@api_view()
@permission_classes([permissions.IsAuthenticated])
def get_jobs(request, id):
    # Getting jobs for the provided archive ID
    archive = Archive.objects.get(pk=id)
    jobs = archive.jobs.all().order_by("start_date")
    
    serializer = JobSerializer(jobs, many=True)
    return Response(serializer.data)


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def harvest(request, recid, source):
    try:
        url = get_source(source).get_record_url(recid)
    except InvalidSource:
        raise BadRequest("Invalid source")

    record, _ = Record.objects.get_or_create(
        recid=recid, source=source, defaults={"url": url}
    )

    archive = Archive.objects.create(
        record=record,
        creator=request.user,
        status=Status.WAITING_APPROVAL,
    )

    job = Job.objects.create(
        archive = archive,
        stage = Stages.HARVEST_REQUESTED,
        status = Status.WAITING_APPROVAL
    )

    return redirect(
        reverse("archive-detail", request=request, kwargs={"pk": archive.id})
    )


@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def upload(request):
    file = request.FILES.getlist('file')[0]

    # WORKAROUND FOR NOW : Get directory name from compressed filename
    # TODO getting source and recid from sip.json?
    sip_dir = file.name.split('.')[0]
    sip_data = sip_dir.split("::")
    source = sip_data[1]
    recid = sip_data[2]

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
        status= Status.IN_PROGRESS
    )

    job = Job.objects.create(
        archive = archive,
        stage = Stages.UPLOADING,
        status = Status.IN_PROGRESS
    )

    # Using root tmp folder
    base_path = os.path.join(os.getcwd(), "tmp")
    try:
        # Save compressed SIP
        compressed_path = os.path.join(base_path, 'compressed.zip') 
        destination = open(compressed_path, 'wb+')
        for chunk in file.chunks():
            destination.write(chunk)
        destination.close()

        # Extract it
        with zipfile.ZipFile(compressed_path, "r") as compressed:
            compressed.extractall(base_path)

        # Remove zip
        os.remove(compressed_path)

        # Uploading completed
        job.set_completed()

        # Save path and change status of the archive
        archive.status = Status.WAITING_APPROVAL
        archive.path_to_sip = os.path.join(base_path, sip_dir)
        archive.save()

        next_job = Job.objects.create(
            archive = archive,
            stage = Stages.VALIDATION_REQUESTED,
            status = Status.WAITING_APPROVAL
        )
    except Exception as e:
        job.set_failed()
        archive.set_failed()

    return Response({"msg" : "SIP uploading started, see Archives page"})


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

@api_view()
@permission_classes([permissions.IsAuthenticated])
def me(request):
    serializer = UserSerializer(request.user)
    return Response(serializer.data)


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
