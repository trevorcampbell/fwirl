from abc import abstractmethod


class GraphWorker:
    """Abstract base class for dynamic graph restructuring callbacks.

    Attach a :class:`GraphWorker` to an :class:`~fwirl.AssetGraph` to receive
    a callback after any of its *watched_assets* are successfully rebuilt.
    Implement :meth:`restructure` to inspect the graph and return new assets
    that should be added before fwirl triggers another build pass.

    This is useful when the set of assets that must be maintained is not
    known up front but is determined by the content of upstream assets (for
    example, discovering a list of files to process from a manifest asset).

    Args:
        watched_assets: An iterable of :class:`~fwirl.Asset` objects whose
            rebuild should trigger :meth:`restructure`.
    """

    def __init__(self, watched_assets):
        self.watched_assets = watched_assets

    @abstractmethod
    async def restructure(self, graph):
        """Inspect the graph and optionally add new assets.

        This coroutine is called after any of the watched assets finish
        rebuilding.  Use the *graph* parameter to add new assets and return
        them so that fwirl can immediately schedule a build for the new nodes.

        Args:
            graph: The live :class:`~fwirl.AssetGraph` instance.

        Returns:
            A list of newly added :class:`~fwirl.Asset` objects, or an empty
            list if no new assets were added.
        """
        pass

