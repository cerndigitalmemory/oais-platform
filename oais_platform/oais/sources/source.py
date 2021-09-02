from abc import ABC, abstractmethod


class Source(ABC):
    @abstractmethod
    def get_record_url(self, recid):
        pass

    @abstractmethod
    def search(self, query, page=1, size=20):
        pass

    @abstractmethod
    def search_by_id(self, recid):
        pass
