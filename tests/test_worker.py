"""Tests for fwirl.worker – GraphWorker base class."""
import pytest

from fwirl.graph import AssetGraph
from fwirl.worker import GraphWorker
from tests.conftest import MemoryAsset, RecordingWorker


class TestGraphWorker:
    def test_cannot_instantiate_abstract(self):
        # GraphWorker has no ABC enforcement; concrete subclasses override
        # restructure(). RecordingWorker is always instantiatable.
        w = RecordingWorker([])
        assert w is not None

    def test_watched_assets_stored(self):
        a = MemoryAsset("a")
        w = RecordingWorker([a])
        assert w.watched_assets == [a]

    async def test_restructure_called_with_graph(self):
        a = MemoryAsset("a")
        w = RecordingWorker([a])
        g = AssetGraph("wg")
        g.add_assets([a])
        result = await w.restructure(g)
        assert result == []
        assert len(w.calls) == 1
        assert w.calls[0] is g

    async def test_restructure_can_add_assets(self):
        a = MemoryAsset("a")
        extra = MemoryAsset("extra")

        def factory(graph):
            return [extra]

        w = RecordingWorker([a], new_assets_factory=factory)
        g = AssetGraph("wg2")
        g.add_assets([a])
        new = await w.restructure(g)
        assert extra in new
        # The factory added extra to the graph
        keys = [asset.key for asset in g.graph.nodes]
        assert "extra" in keys

    async def test_restructure_returns_empty_when_no_factory(self):
        w = RecordingWorker([])
        g = AssetGraph("wg3")
        result = await w.restructure(g)
        assert result == []
