from django.shortcuts import render
import abc

from os import environ

from django.http import HttpResponse
from opensearch_dsl import Q, A, Search
from opensearchpy import OpenSearch
from opensearch_dsl.connections import connections
from oais_platform.oais.exceptions import BadRequest

from rest_framework.pagination import LimitOffsetPagination
from rest_framework.views import APIView

import requests, json

from oais_platform.oais.documents import ArchiveDocument
from oais_platform.oais.serializers import ArchiveSerializer


class PaginatedSearchAPIView(APIView, LimitOffsetPagination):
    serializer_class = None
    document_class = None

    def post(self, request):
        try:
            """
            Gets all the query parameters and the active fields
            and creates an opensearch request
            """
            try:
                user = request.user
                pagination_from = request.data["from"]
                pagination_size = request.data["size"]
            except Exception as e:
                return HttpResponse(status=500)

            try:
                """
                Searches for the query in the title or the recid,
                if query is empty then returns all the results
                """
                query = request.data["query"]
                q = Q("match", title=query) | Q("match", recid=query)
            except KeyError:
                q = Q("match_all")
            except Exception as e:
                return HttpResponse(status=500)

            try:
                source = request.data["source"]
            except KeyError:
                source = None
            except Exception as e:
                return HttpResponse(status=500)

            try:
                restricted = request.data["visibility"]
            except KeyError:
                restricted = None
            except Exception as e:
                return HttpResponse(status=500)

            try:
                steps_name = request.data["steps_name"]
            except KeyError:
                steps_name = None
            except Exception as e:
                return HttpResponse(status=500)

            # Create the client with SSL/TLS enabled, but hostname verification disabled.
            client = OpenSearch(
                hosts=[{"host": "opensearch-node1", "port": 9200}],
            )

            # Search initialization, see https://elasticsearch-dsl.readthedocs.io/en/latest/
            s = Search(using=client).query(q)

            # Add Aggregations
            s.aggs.bucket("sources", "terms", field="source")
            s.aggs.bucket("visibility", "terms", field="restricted")
            s.aggs.bucket("steps_name", "terms", field="steps.name")

            # Filtering
            if source:
                s = s.filter("terms", source=source)

            if restricted == None:
                s = s.filter("term", restricted=False)
            elif len(restricted) == 1:
                if restricted[0] == "0":
                    s = s.filter("term", restricted=False)
                elif restricted[0] == "1":
                    s = s.filter("term", creator__username=user.username)
            elif len(restricted) == 2:
                restricted_filter = Q("match", creator__username=user.username) | Q(
                    "match", restricted=False
                )
                s = s.query("bool", filter=[restricted_filter])
            if steps_name:
                s = s.filter("terms", steps__name=steps_name)

            # Pagination
            s = s[pagination_from : pagination_from + pagination_size]

            # Execute search
            response = s.execute()

            dict_response = response.to_dict()

            return HttpResponse(json.dumps(dict_response))
        except Exception as e:
            raise BadRequest("Error while performing searching: ", e)


class SearchArchives(PaginatedSearchAPIView):
    serializer_class = ArchiveSerializer
    document_class = ArchiveDocument
