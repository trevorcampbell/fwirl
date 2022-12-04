from enum import Enum
from abc import abstractmethod
from loguru import logger
import pendulum as plm

class AssetStatus(Enum):
    Current = 0
    Stale = 1
    Building = 2
    Paused = 3
    UpstreamPaused = 4
    Unavailable = 5
    Failed = 6

class Asset:
    def __init__(self, key, dependencies, group = None, subgroup = None):
        self.key = key
        self.hash = hash(key)
        self.dependencies = dependencies
        self.status = AssetStatus.Unavailable
        self.message = ""
        self.group = group
        self.subgroup = subgroup
        self.viznode = None
        self._cached_timestamp = AssetStatus.Unavailable

    def __hash__(self):
        return self.hash

    def __eq__(self, rhs):
        return self.hash == rhs.hash

    def __repr__(self):
        return self.__class__.__name__ + f"({self.key})"

    def get_key(self):
        return self.key

    def _timestamp(self):
        self._cached_timestamp = self.timestamp()
        return self._cached_timestamp

    @abstractmethod
    def timestamp(self):
        # return timestamp if exists
        # return AssetStatus.Unavailable if not
        pass

    @abstractmethod
    def build(self):
        # if returns an object, gets serialized and stored in db
        pass

# Assets for which we can only obtain a value (no notion of a timestamp)
# may be modified by external agents asynchronously with no notification
# automatically updates timestamps when a new value is obtained that is different from previous value
class ExternalAsset(Asset):
    def _get(self):
        # TODO add error handling; if the asset get fails, then timestamp should be Unavailable, not plm.now
        val = self.get()
        if diff(val, self._cached_val):
            self._cached_val = val
            self._cached_timestamp = plm.now()

    def timestamp(self):
        return self._cached_timestamp

    def build(self):
        pass

    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def diff(self, val1, val2):
        pass

    

