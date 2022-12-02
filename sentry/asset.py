from enum import Enum
from abc import abstractmethod
import uuid

class AssetStatus(Enum):
    Unknown = 0
    Current = 1
    Outdated = 2
    Unavailable = 3
    Failed = 4

class Asset:

    def __init__(self):
        self.status = AssetStatus.Unknown
        self.key = "blah"

    def __hash__(self):
        return self.key

    def get_key(self):
        return self.key

    @abstractmethod
    def get_timestamp(self):
        return

    @abstractmethod
    def build(self):
        return


class ExternalAsset(Asset):

    def build(self):
        pass
    
