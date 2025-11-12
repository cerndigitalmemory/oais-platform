from abc import ABC, abstractmethod


class AbstractSource(ABC):
    @abstractmethod
    def get_record_url(self, recid):
        pass

    @abstractmethod
    def search(self, query, page=1, size=20):
        pass

    @abstractmethod
    def search_by_id(self, recid):
        pass

    def notify_source(self, archive, notification_endpoint, api_key=None):
        raise NotImplementedError("Step Notify Source not implemented for this Source.")

    def get_records_to_harvest(
        self, start, end, size=500, created_filter=False
    ):  # Implementation should use yielding of records
        raise NotImplementedError(
            "Get latest records to harvest not implemented for this Source."
        )
