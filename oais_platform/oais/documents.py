# documents.py
from django.contrib.auth.models import User
from django_opensearch_dsl import Document, fields
from django_opensearch_dsl.registries import registry

from .models import Archive, Step


@registry.register_document
class UserDocument(Document):
    class Index:
        name = "users"
        settings = {
            "number_of_shards": 1,
            "number_of_replicas": 0,
        }

    class Django:
        model = User
        fields = [
            "id",
            "first_name",
            "last_name",
            "username",
        ]


@registry.register_document
class ArchiveDocument(Document):

    creator = fields.ObjectField(
        properties={
            "id": fields.IntegerField(),
            "username": fields.TextField(),
            "permissions": fields.TextField(
                multi=True,
            ),
        },
    )
    last_step = fields.ObjectField()
    source = fields.KeywordField()

    steps = fields.ObjectField(
        properties={
            "id": fields.IntegerField(),
            "name": fields.IntegerField(),
            "status": fields.IntegerField(),
        },
    )

    class Index:
        name = "archive"  # Name of the Opensearch index
        settings = {  # See Opensearch Indices API reference for available settings
            "number_of_shards": 1,
            "number_of_replicas": 0,
        }
        # Configure how the index should be refreshed after an update.
        # See Opensearch documentation for supported options.
        # This per-Document setting overrides settings.OPENSEARCH_DSL_AUTO_REFRESH.
        auto_refresh = False

    class Django:
        model = Archive  # The model associated with this Document
        fields = [  # The fields of the model you want to be indexed in Opensearch
            "id",
            "source_url",
            "recid",
            "title",
            "restricted",
        ]
        # Paginate the Django queryset used to populate the index with the specified size
        # This per-Document setting overrides settings.OPENSEARCH_DSL_QUERYSET_PAGINATION.
        related_models = [Step, User]
        queryset_pagination = 5000

    def get_instances_from_related(self, related_instance):
        """If related_models is set, define how to retrieve the Car instance(s) from the related model.
        The related_models option should be used with caution because it can lead in the index
        to the updating of a lot of items.
        """
        if isinstance(related_instance, Step):
            return related_instance.steps_set.all()
