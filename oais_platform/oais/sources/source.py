from abc import ABC, abstractmethod


class Source(ABC):
    @abstractmethod
    def get_record_url(self, recid):
        pass

    @abstractmethod
    def search(self, query, page=1):
        pass
