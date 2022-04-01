from django.shortcuts import render
import abc

from django.http import HttpResponse
from opensearch_dsl import Q, serializer, Search
from opensearchpy import OpenSearch

from rest_framework.pagination import LimitOffsetPagination
from rest_framework.views import APIView

import requests, json

from oais_platform.oais.documents import ArchiveDocument
from oais_platform.oais.serializers import ArchiveSerializer


class PaginatedSearchAPIView(APIView, LimitOffsetPagination):
    serializer_class = None
    document_class = None

    def post(self, request):
        # try:
        query = request.data

        # Create the client with SSL/TLS enabled, but hostname verification disabled.
        client = OpenSearch(
            hosts=[{"host": "localhost", "port": 9200}],
            http_compress=True,  # enables gzip compression for request bodies
            http_auth=("admin", "admin"),
            # client_cert = client_cert_path,
            # client_key = client_key_path,
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
            # ca_certs = ca_certs_path
        )

        s = Search(using=client, index="archive").from_dict(query)

        response = s.execute()

        dict_response = response.to_dict()

        return HttpResponse(json.dumps(dict_response))


class SearchArchives(PaginatedSearchAPIView):
    serializer_class = ArchiveSerializer
    document_class = ArchiveDocument
