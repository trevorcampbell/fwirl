from abc import abstractmethod
from enum import Enum


class Resource:
    """Abstract base class for a managed build resource.

    Resources represent shared, stateful objects (database connections, file
    handles, hardware locks, etc.) that must be acquired before a build begins
    and released when it finishes.  Attach one or more :class:`Resource`
    instances to an :class:`~fwirl.Asset` via its ``resources`` constructor
    argument; fwirl will call :meth:`init` before the build pass and
    :meth:`close` after it completes (or fails).

    Args:
        key: A unique string identifier for this resource.

    Example::

        import fwirl

        class DBConnection(fwirl.Resource):
            def init(self):
                self.conn = connect_to_db()

            def close(self):
                self.conn.close()
    """

    def __init__(self, key):
        self.hash = hash(key)
        self.key = key

    def __hash__(self):
        return self.hash

    def __eq__(self, rhs):
        return self.hash == rhs.hash

    def __repr__(self):
        return self.__class__.__name__ + f"({self.key})"

    @abstractmethod
    def init(self):
        """Acquire / open the resource.

        Called once before the build pass that needs this resource begins.
        Raise an exception to abort the build.
        """
        pass

    @abstractmethod
    def close(self):
        """Release / close the resource.

        Called after every build pass, even when :meth:`init` raised an
        exception for a *different* resource in the same build.
        """
        pass
