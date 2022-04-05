from django.shortcuts import render
import abc

from django.http import HttpResponse
from opensearch_dsl import Q, A, Search
from opensearchpy import OpenSearch
from opensearch_dsl.connections import connections

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
        # Data initialization, will be from get request
        query = "the"
        source = ["cds"]
        restricted = True
        user = "root"

        # Queries the tiltle and recid fields
        q = Q("match", title=query) | Q("match", recid=query)

        # Search initialization
        s = Search(using=client).query(q)

        # Add Aggregations
        s.aggs.bucket("sources", "terms", field="source").bucket(
            "restricted", "terms", field="restricted"
        ).bucket("visibility", "terms", field="last_step")

        # Filtering
        if source:
            s = s.filter("terms", source=source)
        if restricted == True:
            s = s.filter("term", creator__username=user)
        if restricted == False:
            s = s.filter("term", restricted=False)

        # Execute search
        response = s.execute()

        dict_response = response.to_dict()

        return HttpResponse(json.dumps(dict_response))


class SearchArchives(PaginatedSearchAPIView):
    serializer_class = ArchiveSerializer
    document_class = ArchiveDocument
