"""
OpenAPI schema fragments.

The views describe themselves to `drf-spectacular` with `@extend_schema`.
Everything that is shared between several endpoints, or that cannot be
expressed with a serializer (query parameters and free-form object
responses), lives here to keep the views readable.

Response payloads that *can* be described by a serializer belong in
`serializers.py` instead.
"""

from drf_spectacular.utils import OpenApiParameter

STRING_LIST_RESPONSE = {"type": "array", "items": {"type": "string"}}

# `/users/me/sources/` returns an object keyed by source name, which cannot be
# described by a serializer, so the schema is spelled out here
SOURCE_STATUS_RESPONSE = {
    "type": "object",
    "additionalProperties": {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "string"},
            "longname": {"type": "string"},
            "status": {
                "type": "integer",
                "enum": [1, 2, 3, 4],
                "description": (
                    "1: configured for both public and restricted records, "
                    "2: configured for public records only, "
                    "3: missing mandatory configuration, "
                    "4: invalid configuration"
                ),
            },
        },
        "required": ["id", "name", "longname", "status"],
    },
}

PAGE_SIZE_PARAMETER = OpenApiParameter(
    name="size",
    description="Number of results per page",
    required=False,
    type=int,
    location=OpenApiParameter.QUERY,
)

ARCHIVE_ACCESS_PARAMETER = OpenApiParameter(
    name="access",
    description="Which Archives to consider",
    required=False,
    type=str,
    enum=["all", "owned", "public"],
    default="all",
    location=OpenApiParameter.QUERY,
)

# ArchiveViewSet accepts the literal value "all" on top of a page number, so the
# auto-generated integer parameter has to be overridden
ARCHIVE_PAGE_PARAMETER = OpenApiParameter(
    name="page",
    description=(
        "A page number within the paginated result set, "
        'or "all" to return every result in a single page'
    ),
    required=False,
    type=str,
    location=OpenApiParameter.QUERY,
)

ARCHIVE_LIST_PARAMETERS = [
    ARCHIVE_ACCESS_PARAMETER,
    ARCHIVE_PAGE_PARAMETER,
    PAGE_SIZE_PARAMETER,
]

TAG_LIST_PARAMETERS = [
    OpenApiParameter(
        name="query",
        description="Free text matched against the Tag title",
        required=False,
        type=str,
        location=OpenApiParameter.QUERY,
    ),
    OpenApiParameter(
        name="username",
        description="Username of the Tag creator",
        required=False,
        type=str,
        location=OpenApiParameter.QUERY,
    ),
    OpenApiParameter(
        name="internal",
        description="Whether to restrict the results to internal (job) Tags",
        required=False,
        type=str,
        enum=["only", "false"],
        location=OpenApiParameter.QUERY,
    ),
    PAGE_SIZE_PARAMETER,
]
