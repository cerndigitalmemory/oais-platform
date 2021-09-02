import logging
import os, zipfile, time
from django.contrib import auth
from django.contrib.auth.models import Group, User
from django.db import transaction
from django.db.models import base
from django.shortcuts import redirect
from oais_platform.oais.exceptions import BadRequest
from oais_platform.oais.mixins import PaginationMixin
from oais_platform.oais.models import Archive, ArchiveStage, ArchiveStatus, Record
from oais_platform.oais.permissions import filter_archives_by_user_perms
from oais_platform.oais.serializers import (ArchiveSerializer, GroupSerializer,
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
        archives = filter_archives_by_user_perms(
            user.archives.all(), request.user)
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
        archives = filter_archives_by_user_perms(
            record.archives.all(), request.user)
        return self.make_paginated_response(archives, ArchiveSerializer)


class ArchiveViewSet(viewsets.ReadOnlyModelViewSet):
    queryset = Archive.objects.all().order_by("-creation_date")
    serializer_class = ArchiveSerializer
    permission_classes = [permissions.IsAuthenticated]

    def get_queryset(self):
        return filter_archives_by_user_perms(
            super().get_queryset(), self.request.user)

    def approve_or_reject(self, request, permission, approved):
        user = request.user
        if not user.has_perm(permission):
            raise PermissionDenied()

        # Make sure the status of the archive is read and updated atomically,
        # otherwise multiple harvesting task might be scheduled.
        with transaction.atomic():
            archive = self.get_object()
            if archive.status != ArchiveStatus.WAITING_APPROVAL:
                raise BadRequest("Archive is not waiting for approval")
            if approved:
                archive.status = ArchiveStatus.PENDING
            else:
                archive.status = ArchiveStatus.REJECTED
            archive.save()

        if approved:
            if archive.stage == ArchiveStage.WAITING_HARVEST:
                process.delay(archive.id)
                validate.delay(archive.id, archive.path_to_sip)
            elif archive.stage == ArchiveStage.SIP_EXISTS:
                validate.delay(archive.id, archive.path_to_sip)

        serializer = self.get_serializer(archive)
        return Response(serializer.data)

    @action(detail=True, methods=["POST"], url_path="actions/approve")
    def approve(self, request, pk=None):
        return self.approve_or_reject(
            request, "oais.can_approve_archive", approved=True)

    @action(detail=True, methods=["POST"], url_path="actions/reject")
    def reject(self, request, pk=None):
        return self.approve_or_reject(
            request, "oais.can_reject_archive", approved=False)


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

    return redirect(
        reverse("archive-detail", request=request, kwargs={"pk": archive.id}))

@api_view(["POST"])
@permission_classes([permissions.IsAuthenticated])
def upload(request):
    file = request.FILES.getlist('file')[0]

    base_path = os.path.join(os.getcwd(), "tmp")
    
    # Save compressed SIP
    compressed_path = os.path.join(base_path, 'compressed.zip') 
    destination = open(compressed_path, 'wb+')
    for chunk in file.chunks():
        destination.write(chunk)
    destination.close()

    # Extract it
    with zipfile.ZipFile(compressed_path, "r") as compressed:
        compressed.extractall(base_path)

    os.remove(compressed_path)

    # Get directory name from compressed filename
    sip_dir = file.name.split('.')[0]

    ## Will be useful when sip.json is according to spec
    # Finding sip.json and extracting information from it
    '''
    sip_path = os.path.join(base_path, sip_dir)
    sip_data_path = os.path.join(sip_path, "data")

    for name in os.listdir(sip_data_path):
        abs_path = os.path.join(sip_data_path, name)
        if os.path.isdir(abs_path):
            for filename in os.listdir(abs_path):
                if filename == "sip.json":
                    sip_json_path = os.path.join(abs_path, filename)
                    print(sip_json_path)
    '''

    # WORKAROUND FOR NOW
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
        stage=ArchiveStage.SIP_EXISTS,
        path_to_sip = os.path.join(base_path, sip_dir)
    )

    return Response({"msg" : "SIP uploaded waiting for approval, see Archives page"})


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
