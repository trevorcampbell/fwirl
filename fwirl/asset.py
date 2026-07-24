from enum import Enum
from abc import abstractmethod
from loguru import logger
import pendulum as plm


class AssetStatus(Enum):
    """Enumeration of possible states for an :class:`Asset`.

    Attributes:
        Current: The asset is up-to-date with respect to all its dependencies.
        Stale: The asset exists but is out-of-date and needs to be rebuilt.
        Building: The asset is currently being built.
        Paused: The asset has been manually paused and will not be built.
        UpstreamStopped: A dependency is paused or failed, blocking this asset.
        Unavailable: The asset has not yet been built or does not exist.
        Failed: The most recent build attempt for this asset failed.
    """

    Current = 0
    Stale = 1
    Building = 2
    Paused = 3
    UpstreamStopped = 4
    Unavailable = 5
    Failed = 6


class Asset:
    """Abstract base class for a single node in an :class:`~fwirl.AssetGraph`.

    Subclass :class:`Asset` and implement the :meth:`timestamp` and
    :meth:`build` coroutines to define how fwirl should check whether an
    asset is up-to-date and how to (re)build it.

    Args:
        key: A unique string identifier for this asset.
        dependencies: A list of :class:`Asset` objects that must be
            up-to-date before this asset can be built.
        resources: Optional list of :class:`~fwirl.Resource` objects that
            will be initialized before and cleaned up after every build.
        group: Optional grouping label used for display purposes.
        subgroup: Optional sub-grouping label used for display purposes.
        allow_retry: When ``True`` (the default) a failed asset will be
            retried on the next build pass; when ``False`` a failed asset
            stays in the ``Failed`` state until manually unpaused.

    Example::

        import fwirl
        import pendulum as plm

        class MyAsset(fwirl.Asset):
            async def timestamp(self):
                # Return the time the asset was last produced, or
                # AssetStatus.Unavailable if it does not yet exist.
                ...

            async def build(self):
                # Produce the asset.
                ...
    """

    def __init__(self, key, dependencies, resources=None, group=None, subgroup=None, allow_retry=True):
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
        """Return the string key that uniquely identifies this asset.

        Returns:
            str: The asset's key.
        """
        return self.key

    @abstractmethod
    async def timestamp(self):
        """Return the timestamp of the most recently produced asset.

        Returns:
            A :class:`pendulum.DateTime` representing when the asset was
            last built, or :attr:`AssetStatus.Unavailable` if the asset
            does not yet exist.
        """
        pass

    @abstractmethod
    async def build(self):
        """Produce (or re-produce) this asset.

        This coroutine is called by fwirl whenever the asset is stale or
        unavailable.  Any return value is ignored; indicate failure by
        raising an exception.
        """
        pass


# Assets for which we can only obtain a value (no notion of a timestamp)
# may be modified by external agents asynchronously with no notification
# automatically updates timestamps when a new value is obtained that is different from previous value
class ExternalAsset(Asset):
    """An :class:`Asset` whose value is managed by an external system.

    Use :class:`ExternalAsset` when the underlying data can change at any
    time outside of fwirl's control (for example, a file written by another
    process, or a row in a database updated by a third-party service).
    fwirl polls the value at most once every *min_polling_interval*; if the
    polled value differs from the cached value the asset is automatically
    marked stale so that downstream assets are rebuilt.

    Subclasses must implement :meth:`get` (to retrieve the current value)
    and :meth:`diff` (to decide whether the new value counts as a change).

    Args:
        key: A unique string identifier for this asset.
        dependencies: A list of :class:`Asset` objects that must be
            up-to-date before this asset can be built.
        min_polling_interval: A :class:`pendulum.Duration` specifying the
            minimum time between consecutive polls of the external resource.
        resources: Optional list of :class:`~fwirl.Resource` objects.
        group: Optional grouping label.
        subgroup: Optional sub-grouping label.
        allow_retry: See :class:`Asset`.
    """

    def __init__(self, key, dependencies, min_polling_interval, resources=None, group=None, subgroup=None, allow_retry=True):
        self.min_polling_interval = min_polling_interval
        self.last_poll = AssetStatus.Unavailable
        self._cached_timestamp = AssetStatus.Unavailable
        self._cached_val = AssetStatus.Unavailable
        self._pending_val = None
        super(ExternalAsset, self).__init__(key, dependencies, resources=resources, group=group, subgroup=subgroup, allow_retry=allow_retry)

    # TODO also add a put method and allow this program to update the external resource
    # TODO self.get error handling?
    # TODO store val/timestamp in a DB to record last poll/val to avoid rerunning flows unnecessarily if this program quits
    async def timestamp(self):
        """Poll the external resource and return the cached timestamp.

        If the minimum polling interval has elapsed, :meth:`get` is called.
        When :meth:`diff` reports a change the asset is marked stale and
        the new value is staged for the next :meth:`build` call.

        Returns:
            A :class:`pendulum.DateTime` of the last accepted value, or
            :attr:`AssetStatus.Unavailable` if no value has been accepted yet.
        """
        if (self.last_poll == AssetStatus.Unavailable) or (plm.now() >= self.last_poll + self.min_polling_interval):
            val = await self.get()
            self.last_poll = plm.now()
            # if there's a diff, flag the asset as stale and store the pending value
            if self.diff(val):
                self._pending_val = val
                if self.status != AssetStatus.Unavailable:
                    self.status = AssetStatus.Stale
        return self._cached_timestamp

    async def build(self):
        """Commit any pending external value and update the internal timestamp."""
        # if there's a pending value, move it into the cached value and update the timestamp
        if self._pending_val is not None:
            self._cached_val = self._pending_val
            self._cached_timestamp = plm.now()
            self._pending_val = None
        return

    @abstractmethod
    async def get(self):
        """Retrieve the current value of the external resource.

        Returns:
            The current value in whatever form is appropriate for
            :meth:`diff` to compare against.
        """
        pass

    @abstractmethod
    def diff(self, val):
        """Return ``True`` if *val* differs from the cached value.

        Args:
            val: The value returned by the most recent :meth:`get` call.

        Returns:
            bool: ``True`` when the asset should be considered stale.
        """
        pass  # compare to self._cached_val

