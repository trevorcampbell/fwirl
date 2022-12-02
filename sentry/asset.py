from enum import Enum
from abc import abstractmethod

class AssetStatus(Enum):
    Current = 0
    Stale = 1
    Building = 2
    Paused = 3
    UpstreamPaused = 4
    Unavailable = 5
    Failed = 6

class Asset:
    def __init__(self):
        self.status = AssetStatus.Unavailable
        self.message = ""
        self.key = "blah"
        self._cached_timestamp = None

    def __hash__(self):
        return self.key

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

# assets that are may be modified asynchronously by external actors
class ExposedAsset(Asset):

    def build(self):
        pass

    def get_timestamp(self):
        # compare value
        # return timestamp if exists
        # return AssetStatus.Unavailable if not
        return

# assets that are just internal to sentry and cannot be modified by external actors
# i.e. serializable, stored in our DB
class InternalAsset(Asset):
    pass



