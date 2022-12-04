from enum import Enum
from abc import abstractmethod
from loguru import logger

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
        return

    @abstractmethod
    def build(self):
        # if returns an object, gets serialized and stored in db
        return

## assets that are may be modified asynchronously by external actors
#class ExposedAsset(Asset):
#
#    def build(self):
#        pass
#
#    def get_timestamp(self):
#        # compare value
#        # return timestamp if exists
#        # return AssetStatus.Unavailable if not
#        return
#
## assets that are just internal to sentry and cannot be modified by external actors
## i.e. serializable, stored in our DB
#class InternalAsset(Asset):
#    pass



