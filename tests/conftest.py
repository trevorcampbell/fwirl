"""Shared fixtures and concrete helper classes for the fwirl test suite.

All concrete asset/resource/worker implementations used across multiple test
modules live here so that individual test files stay focused on behaviour.
"""
import asyncio

import pendulum as plm
import pytest

from fwirl.asset import Asset, AssetStatus, ExternalAsset
from fwirl.graph import AssetGraph
from fwirl.resource import Resource
from fwirl.worker import GraphWorker


# ---------------------------------------------------------------------------
# Concrete Asset implementations
# ---------------------------------------------------------------------------

class MemoryAsset(Asset):
    """An Asset whose timestamp and content live only in memory.

    Parameters
    ----------
    key:
        Unique asset key.
    dependencies:
        Upstream assets (default: none).
    initial_ts:
        Initial timestamp.  Pass ``AssetStatus.Unavailable`` (default) to
        start the asset in an unavailable state.
    build_raises:
        When *True*, :meth:`build` raises a ``RuntimeError``.
    build_delay:
        Seconds to sleep inside :meth:`build` (useful for concurrency tests).
    """

    def __init__(self, key, dependencies=None, initial_ts=AssetStatus.Unavailable,
                 build_raises=False, build_delay=0.0, **kwargs):
        super().__init__(key, dependencies or [], **kwargs)
        self._ts = initial_ts
        self._build_raises = build_raises
        self._build_delay = build_delay
        self.build_count = 0

    async def timestamp(self):
        return self._ts

    async def build(self):
        if self._build_delay:
            await asyncio.sleep(self._build_delay)
        if self._build_raises:
            raise RuntimeError("intentional build failure")
        self.build_count += 1
        self._ts = plm.now()


class SlowTimestampAsset(Asset):
    """Asset whose timestamp() introduces an async delay."""

    def __init__(self, key, dependencies=None, ts=AssetStatus.Unavailable, delay=0.01):
        super().__init__(key, dependencies or [])
        self._ts = ts
        self._delay = delay

    async def timestamp(self):
        await asyncio.sleep(self._delay)
        return self._ts

    async def build(self):
        await asyncio.sleep(self._delay)
        self._ts = plm.now()


class SimpleExternalAsset(ExternalAsset):
    """Concrete ExternalAsset that wraps a mutable Python value."""

    def __init__(self, key, value=None, **kwargs):
        super().__init__(key, [], min_polling_interval=plm.duration(seconds=0), **kwargs)
        self._value = value

    async def get(self):
        return self._value

    def diff(self, val):
        return val != self._cached_val

    def set_value(self, v):
        self._value = v


# ---------------------------------------------------------------------------
# Concrete Resource implementation
# ---------------------------------------------------------------------------

class CountingResource(Resource):
    """Resource that records how many times it has been opened/closed."""

    def __init__(self, key="counting-resource", init_raises=False, close_raises=False):
        super().__init__(key)
        self.init_count = 0
        self.close_count = 0
        self._init_raises = init_raises
        self._close_raises = close_raises

    def init(self):
        if self._init_raises:
            raise RuntimeError("resource init failed")
        self.init_count += 1

    def close(self):
        if self._close_raises:
            raise RuntimeError("resource close failed")
        self.close_count += 1


# ---------------------------------------------------------------------------
# Concrete GraphWorker implementation
# ---------------------------------------------------------------------------

class RecordingWorker(GraphWorker):
    """Worker that records each restructure call and optionally adds new assets."""

    def __init__(self, watched_assets, new_assets_factory=None):
        super().__init__(watched_assets)
        self.calls = []
        self._new_assets_factory = new_assets_factory  # callable(graph) -> [Asset]

    async def restructure(self, graph):
        self.calls.append(graph)
        if self._new_assets_factory:
            new = self._new_assets_factory(graph)
            if new:
                graph.add_assets(new)
            return new
        return []


# ---------------------------------------------------------------------------
# Common fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def simple_graph():
    """A fresh AssetGraph with key 'test-graph'."""
    return AssetGraph("test-graph")


@pytest.fixture
def linear_assets():
    """Three MemoryAssets in a chain: A -> B -> C (all unavailable)."""
    a = MemoryAsset("A")
    b = MemoryAsset("B", dependencies=[a])
    c = MemoryAsset("C", dependencies=[b])
    return a, b, c


@pytest.fixture
def populated_graph(simple_graph, linear_assets):
    """AssetGraph with assets A -> B -> C already added."""
    a, b, c = linear_assets
    simple_graph.add_assets([c])  # adding leaf pulls in all ancestors
    return simple_graph, a, b, c
