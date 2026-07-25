"""Tests for AssetGraph._build_asset and full build passes."""
import asyncio

import pendulum as plm
import pytest

from fwirl.asset import AssetStatus
from fwirl.graph import AssetGraph, BuildResult
from tests.conftest import CountingResource, MemoryAsset, RecordingWorker


def _graph_with(*assets):
    g = AssetGraph("build-test")
    g.add_assets(list(assets))
    return g


# ---------------------------------------------------------------------------
# _build_asset unit tests
# ---------------------------------------------------------------------------

class TestBuildAsset:
    async def test_skips_current_asset(self):
        a = MemoryAsset("a", initial_ts=plm.now())
        a.status = AssetStatus.Current
        g = _graph_with(a)
        result = await g._build_asset(a)
        assert result == BuildResult.Skipped
        assert a.build_count == 0

    async def test_skips_paused_asset(self):
        a = MemoryAsset("a")
        a.status = AssetStatus.Paused
        g = _graph_with(a)
        result = await g._build_asset(a)
        assert result == BuildResult.Skipped

    async def test_skips_when_parent_not_current(self):
        parent = MemoryAsset("parent")
        parent.status = AssetStatus.Unavailable
        child = MemoryAsset("child", dependencies=[parent])
        child.status = AssetStatus.Unavailable
        g = _graph_with(child)
        result = await g._build_asset(child)
        assert result == BuildResult.Skipped

    async def test_builds_unavailable_root_asset(self):
        a = MemoryAsset("a")
        a.status = AssetStatus.Unavailable
        g = _graph_with(a)
        result = await g._build_asset(a)
        assert result == BuildResult.Rebuilt
        assert a.status == AssetStatus.Current
        assert a.build_count == 1

    async def test_builds_stale_root_asset(self):
        a = MemoryAsset("a", initial_ts=plm.now().subtract(hours=1))
        a.status = AssetStatus.Stale
        g = _graph_with(a)
        result = await g._build_asset(a)
        assert result == BuildResult.Rebuilt

    async def test_failed_build_sets_failed_status(self):
        a = MemoryAsset("a", build_raises=True)
        a.status = AssetStatus.Unavailable
        g = _graph_with(a)
        result = await g._build_asset(a)
        assert result == BuildResult.Failed
        assert a.status == AssetStatus.Failed

    async def test_build_sets_message_on_success(self):
        a = MemoryAsset("a")
        a.status = AssetStatus.Unavailable
        g = _graph_with(a)
        await g._build_asset(a)
        assert a.message != ""

    async def test_build_sets_message_on_failure(self):
        a = MemoryAsset("a", build_raises=True)
        a.status = AssetStatus.Unavailable
        g = _graph_with(a)
        await g._build_asset(a)
        assert "failed" in a.message.lower()

    async def test_build_child_after_parent_current(self):
        parent = MemoryAsset("parent")
        parent.status = AssetStatus.Current
        parent._ts = plm.now().subtract(seconds=1)
        child = MemoryAsset("child", dependencies=[parent])
        child.status = AssetStatus.Unavailable
        g = _graph_with(child)
        result = await g._build_asset(child)
        assert result == BuildResult.Rebuilt
        assert child.status == AssetStatus.Current


# ---------------------------------------------------------------------------
# Full build pass
# ---------------------------------------------------------------------------

class TestFullBuild:
    def test_build_all_unavailable(self):
        a = MemoryAsset("a")
        b = MemoryAsset("b", dependencies=[a])
        g = _graph_with(b)
        g.build()
        assert a.status == AssetStatus.Current
        assert b.status == AssetStatus.Current

    def test_build_only_upstream_dependencies(self):
        a = MemoryAsset("a")
        b = MemoryAsset("b", dependencies=[a])
        c = MemoryAsset("c", dependencies=[b])
        d = MemoryAsset("d")  # independent branch
        g = _graph_with(c)
        g.add_assets([d])
        g.build(assets=c)
        # c and its ancestors should be built; d should remain Unavailable
        assert c.status == AssetStatus.Current
        assert d.status == AssetStatus.Unavailable

    def test_build_does_not_rebuild_current_assets(self):
        a = MemoryAsset("a", initial_ts=plm.now())
        a.status = AssetStatus.Current
        g = _graph_with(a)
        g.build()
        assert a.build_count == 0

    def test_build_failed_asset_is_reported(self):
        a = MemoryAsset("a", build_raises=True)
        g = _graph_with(a)
        g.build()
        assert a.status == AssetStatus.Failed

    def test_build_paused_asset_skipped(self):
        a = MemoryAsset("a")
        a.status = AssetStatus.Paused
        g = _graph_with(a)
        g.build()
        assert a.status == AssetStatus.Paused
        assert a.build_count == 0

    def test_build_with_resources(self):
        r = CountingResource()
        a = MemoryAsset("a", resources=[r])
        g = _graph_with(a)
        g.build()
        assert r.init_count == 1
        assert r.close_count == 1

    def test_build_cleans_up_resources_after_failure(self):
        r = CountingResource()
        a = MemoryAsset("a", build_raises=True, resources=[r])
        g = _graph_with(a)
        g.build()
        assert r.close_count == 1

    def test_build_aborts_when_resource_init_fails(self):
        r = CountingResource(init_raises=True)
        a = MemoryAsset("a", resources=[r])
        g = _graph_with(a)
        g.build()
        # Build should have been aborted; asset stays Unavailable
        assert a.status == AssetStatus.Unavailable
        assert a.build_count == 0


# ---------------------------------------------------------------------------
# Build with GraphWorker
# ---------------------------------------------------------------------------

class TestBuildWithWorker:
    def test_worker_triggered_when_watched_asset_built(self):
        a = MemoryAsset("a")
        w = RecordingWorker([a])
        g = _graph_with(a)
        g.workers.append(w)
        g.build()
        assert len(w.calls) == 1

    def test_worker_not_triggered_when_asset_already_current(self):
        a = MemoryAsset("a", initial_ts=plm.now())
        a.status = AssetStatus.Current
        w = RecordingWorker([a])
        g = _graph_with(a)
        g.workers.append(w)
        g.build()
        assert len(w.calls) == 0

    def test_worker_adds_new_asset(self):
        a = MemoryAsset("a")
        extra = MemoryAsset("extra", dependencies=[a])

        def factory(graph):
            if any(n.key == "extra" for n in graph.graph.nodes):
                return []
            graph.add_assets([extra])
            return [extra]

        w = RecordingWorker([a], new_assets_factory=factory)
        g = _graph_with(a)
        g.workers.append(w)
        g.build()
        keys = {n.key for n in g.graph.nodes}
        assert "extra" in keys


# ---------------------------------------------------------------------------
# Concurrency – build with async delays
# ---------------------------------------------------------------------------

class TestConcurrentBuild:
    def test_parallel_build_of_independent_assets(self):
        """Two independent assets with build delays should both complete."""
        a = MemoryAsset("a", build_delay=0.05)
        b = MemoryAsset("b", build_delay=0.05)
        g = AssetGraph("concurrent-test")
        g.add_assets([a, b])
        g.build()
        assert a.status == AssetStatus.Current
        assert b.status == AssetStatus.Current

    def test_dependent_build_respects_ordering(self):
        """Parent must be built before child even with delays."""
        a = MemoryAsset("a", build_delay=0.02)
        b = MemoryAsset("b", dependencies=[a], build_delay=0.02)
        g = _graph_with(b)
        g.build()
        assert a.status == AssetStatus.Current
        assert b.status == AssetStatus.Current
        # child's timestamp should be >= parent's
        assert b._ts >= a._ts
