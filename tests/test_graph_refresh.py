"""Tests for AssetGraph._refresh_asset – all status-transition rules."""
import asyncio

import pendulum as plm
import pytest

from fwirl.asset import AssetStatus
from fwirl.graph import AssetGraph
from tests.conftest import MemoryAsset


def _graph_with(*assets):
    g = AssetGraph("refresh-test")
    g.add_assets(list(assets))
    return g


# ---------------------------------------------------------------------------
# Root asset (no parents)
# ---------------------------------------------------------------------------

class TestRefreshRootAsset:
    async def test_unavailable_when_no_timestamp(self):
        a = MemoryAsset("a")
        g = _graph_with(a)
        await g._refresh_asset(a)
        assert a.status == AssetStatus.Unavailable

    async def test_current_when_timestamp_exists(self):
        a = MemoryAsset("a", initial_ts=plm.now())
        g = _graph_with(a)
        await g._refresh_asset(a)
        assert a.status == AssetStatus.Current

    async def test_paused_asset_stays_paused(self):
        a = MemoryAsset("a", initial_ts=plm.now())
        a.status = AssetStatus.Paused
        g = _graph_with(a)
        await g._refresh_asset(a)
        assert a.status == AssetStatus.Paused

    async def test_stale_asset_stays_stale(self):
        a = MemoryAsset("a", initial_ts=plm.now())
        a.status = AssetStatus.Stale
        g = _graph_with(a)
        await g._refresh_asset(a)
        assert a.status == AssetStatus.Stale

    async def test_failed_with_allow_retry_becomes_stale(self):
        a = MemoryAsset("a", initial_ts=plm.now(), allow_retry=True)
        a.status = AssetStatus.Failed
        g = _graph_with(a)
        await g._refresh_asset(a)
        assert a.status == AssetStatus.Stale

    async def test_failed_without_allow_retry_stays_failed(self):
        a = MemoryAsset("a", initial_ts=plm.now(), allow_retry=False)
        a.status = AssetStatus.Failed
        g = _graph_with(a)
        await g._refresh_asset(a)
        assert a.status == AssetStatus.Failed


# ---------------------------------------------------------------------------
# Asset with parents
# ---------------------------------------------------------------------------

class TestRefreshWithParents:
    async def test_unavailable_when_parent_current_but_self_unavailable(self):
        parent = MemoryAsset("parent", initial_ts=plm.now())
        parent.status = AssetStatus.Current
        child = MemoryAsset("child", dependencies=[parent])
        g = _graph_with(child)
        await g._refresh_asset(child)
        assert child.status == AssetStatus.Unavailable

    async def test_stale_when_parent_is_unavailable(self):
        parent = MemoryAsset("parent")  # no timestamp
        parent.status = AssetStatus.Unavailable
        child = MemoryAsset("child", dependencies=[parent], initial_ts=plm.now())
        g = _graph_with(child)
        await g._refresh_asset(child)
        assert child.status == AssetStatus.Stale

    async def test_stale_when_parent_is_stale(self):
        parent = MemoryAsset("parent", initial_ts=plm.now())
        parent.status = AssetStatus.Stale
        child = MemoryAsset("child", dependencies=[parent], initial_ts=plm.now())
        g = _graph_with(child)
        await g._refresh_asset(child)
        assert child.status == AssetStatus.Stale

    async def test_upstream_stopped_when_parent_is_paused(self):
        parent = MemoryAsset("parent", initial_ts=plm.now())
        parent.status = AssetStatus.Paused
        child = MemoryAsset("child", dependencies=[parent], initial_ts=plm.now())
        g = _graph_with(child)
        await g._refresh_asset(child)
        assert child.status == AssetStatus.UpstreamStopped

    async def test_upstream_stopped_when_parent_is_failed(self):
        parent = MemoryAsset("parent", initial_ts=plm.now())
        parent.status = AssetStatus.Failed
        child = MemoryAsset("child", dependencies=[parent], initial_ts=plm.now())
        g = _graph_with(child)
        await g._refresh_asset(child)
        assert child.status == AssetStatus.UpstreamStopped

    async def test_upstream_stopped_when_parent_is_upstream_stopped(self):
        parent = MemoryAsset("parent", initial_ts=plm.now())
        parent.status = AssetStatus.UpstreamStopped
        child = MemoryAsset("child", dependencies=[parent], initial_ts=plm.now())
        g = _graph_with(child)
        await g._refresh_asset(child)
        assert child.status == AssetStatus.UpstreamStopped

    async def test_stale_when_child_older_than_parent(self):
        old_ts = plm.now().subtract(hours=1)
        new_ts = plm.now()
        parent = MemoryAsset("parent", initial_ts=new_ts)
        parent.status = AssetStatus.Current
        child = MemoryAsset("child", dependencies=[parent], initial_ts=old_ts)
        g = _graph_with(child)
        await g._refresh_asset(child)
        assert child.status == AssetStatus.Stale

    async def test_current_when_child_newer_than_parent(self):
        old_ts = plm.now().subtract(hours=1)
        new_ts = plm.now()
        parent = MemoryAsset("parent", initial_ts=old_ts)
        parent.status = AssetStatus.Current
        child = MemoryAsset("child", dependencies=[parent], initial_ts=new_ts)
        g = _graph_with(child)
        await g._refresh_asset(child)
        assert child.status == AssetStatus.Current


# ---------------------------------------------------------------------------
# Full graph refresh
# ---------------------------------------------------------------------------

class TestFullRefresh:
    def test_refresh_all_marks_unavailable(self):
        a = MemoryAsset("a")
        b = MemoryAsset("b", dependencies=[a])
        g = _graph_with(b)
        g.refresh()
        assert a.status == AssetStatus.Unavailable
        assert b.status == AssetStatus.Unavailable

    def test_refresh_current_root(self):
        a = MemoryAsset("a", initial_ts=plm.now())
        b = MemoryAsset("b", dependencies=[a], initial_ts=plm.now())
        a.status = AssetStatus.Current
        g = _graph_with(b)
        g.refresh()
        assert a.status == AssetStatus.Current

    def test_refresh_specific_asset(self):
        a = MemoryAsset("a", initial_ts=plm.now())
        b = MemoryAsset("b", dependencies=[a], initial_ts=plm.now())
        a.status = AssetStatus.Current
        g = _graph_with(b)
        g.refresh(assets=b)  # pass single asset, not list
        assert b.status in {AssetStatus.Current, AssetStatus.Stale, AssetStatus.Unavailable}

    def test_refresh_respects_resources(self):
        from tests.conftest import CountingResource
        r = CountingResource()
        a = MemoryAsset("a", resources=[r])
        g = _graph_with(a)
        g.refresh()
        assert r.init_count == 1
        assert r.close_count == 1

    def test_refresh_resource_init_failure_aborts(self):
        from tests.conftest import CountingResource
        r = CountingResource(init_raises=True)
        a = MemoryAsset("a", resources=[r])
        g = _graph_with(a)
        g.refresh()  # should not raise; init failure aborts gracefully
        # Status is unchanged (still Unavailable)
        assert a.status == AssetStatus.Unavailable
