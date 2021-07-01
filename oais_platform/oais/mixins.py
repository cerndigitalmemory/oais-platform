from rest_framework.response import Response


class PaginationMixin:
    def make_paginated_response(self, queryset, serializer_class):
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = serializer_class(
                page, context={"request": self.request}, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = serializer_class(
            queryset, context={"request": self.request}, many=True)
        return Response(serializer.data)
