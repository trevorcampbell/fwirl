from enum import Enum
from abc import abstractmethod
from loguru import logger
import pendulum as plm

class AssetStatus(Enum):
    Current = 0
    Stale = 1
    Building = 2
    Paused = 3
    UpstreamStopped = 4
    Unavailable = 5
    Failed = 6

class Asset:
    def __init__(self, key, dependencies, resources = None, group = None, subgroup = None, allow_retry = True):
        self.key = key
        self.hash = hash(key)
        self.dependencies = dependencies
        self.resources = [] if (resources is None) else resources
        self.status = AssetStatus.Unavailable
        self.message = ""
        self.group = group
        self.subgroup = subgroup
        self.allow_retry = allow_retry
        self._last_build_timestamp = AssetStatus.Unavailable

    def __hash__(self):
        return self.hash

    def __eq__(self, rhs):
        return self.hash == rhs.hash

    def __repr__(self):
        #return self.__class__.__name__ + f"({self.key})"
        return self.key

    def get_key(self):
        return self.key

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
    def __init__(self, key, dependencies, min_polling_interval, resources = None, group = None, subgroup = None, allow_retry = True):
        self.min_polling_interval = min_polling_interval
        self.last_poll = AssetStatus.Unavailable
        self._cached_timestamp = AssetStatus.Unavailable
        self._cached_val = AssetStatus.Unavailable
        super(ExternalAsset,self).__init__(key, dependencies, resources=resources, group=group, subgroup=subgroup, allow_retry=allow_retry)

    # TODO also add a put method and allow this program to update the external resource
    # TODO self.get error handling?
    # TODO store val/timestamp in a DB to record last poll/val to avoid rerunning flows unnecessarily if this program quits
    def timestamp(self):
        if (self.last_poll == AssetStatus.Unavailable) or (plm.now() >= self.last_poll + min_polling_interval):
            val = self.get()
            self.last_poll = plm.now()
            if diff(val, self._cached_val):
                self._cached_val = val
                return plm.now()
        return self._cached_timestamp

    def build(self):
        pass

    @abstractmethod
    def get(self):
        pass

    @abstractmethod
    def diff(self, val1, val2):
        pass

# This asset is just used internally for doing graph searches by key
class _UnusedAsset(Asset):
    def __init__(self, key):
        super(_UnusedAsset,self).__init__(key, [])
        self.status = "__UNUSED_ASSET_NO_STATUS__" #just to flag that this node is not to be used as an actual asset, just for graph lookup

    def timestamp(self):
        pass

    def build(self):
        pass
