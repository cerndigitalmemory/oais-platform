from rest_framework.response import Response


class PaginationMixin:
    def make_paginated_response(self, queryset, serializer_class, extra_context=None):
        page = self.paginate_queryset(queryset)
        context = {"request": self.request}
        if extra_context:
            context |= extra_context
        if page is not None:
            serializer = serializer_class(page, context=context, many=True)
            return self.get_paginated_response(serializer.data)

        serializer = serializer_class(queryset, context=context, many=True)
        return Response(serializer.data)
