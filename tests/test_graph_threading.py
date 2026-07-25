"""Tests for thread-safety and async concurrency in AssetGraph.

These tests exercise the threading boundary between the message-queue thread
and the asyncio event loop, the _initialize_resources / _cleanup_resources
logic under concurrent access, and async task interleaving during build and
refresh passes.
"""
import asyncio
import threading
import time
from queue import Queue
from unittest.mock import patch

import pendulum as plm
import pytest

from fwirl.asset import AssetStatus
from fwirl.graph import AssetGraph
from tests.conftest import CountingResource, MemoryAsset


# ---------------------------------------------------------------------------
# Thread-safety of the message queue
# ---------------------------------------------------------------------------

class TestMessageQueueThreadSafety:
    def test_concurrent_producers_all_messages_delivered(self):
        """Many threads writing to the message_queue simultaneously."""
        g = AssetGraph("thread-test")
        n = 50
        errors = []

        def producer(i):
            try:
                g.message_queue.put({"type": "noop", "i": i})
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=producer, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        assert g.message_queue.qsize() == n

    def test_concurrent_get_and_put(self):
        """Simultaneous get and put don't deadlock or lose messages."""
        g = AssetGraph("thread-test-2")
        received = []
        done = threading.Event()

        def consumer():
            for _ in range(10):
                item = g.message_queue.get(timeout=2)
                received.append(item)
            done.set()

        t = threading.Thread(target=consumer, daemon=True)
        t.start()
        for i in range(10):
            g.message_queue.put(i)
        assert done.wait(timeout=5), "Consumer timed out"
        assert len(received) == 10


# ---------------------------------------------------------------------------
# Concurrent refresh tasks (async)
# ---------------------------------------------------------------------------

class TestConcurrentRefreshAsync:
    async def test_many_independent_assets_refresh_concurrently(self):
        """Refresh 20 independent assets; all should finish correctly."""
        assets = [MemoryAsset(f"a{i}", initial_ts=plm.now()) for i in range(20)]
        g = AssetGraph("async-refresh-test")
        g.add_assets(assets)
        await g._refresh(assets=None)
        assert all(a.status == AssetStatus.Current for a in assets)

    async def test_chain_refresh_respects_dependency_order(self):
        """In a long chain each node should see its parent's final status."""
        head = MemoryAsset("head", initial_ts=plm.now())
        prev = head
        chain = [head]
        for i in range(9):
            node = MemoryAsset(f"n{i}", dependencies=[prev],
                               initial_ts=plm.now())
            chain.append(node)
            prev = node
        leaf = chain[-1]
        g = AssetGraph("chain-refresh")
        g.add_assets([leaf])
        await g._refresh(assets=None)
        assert all(n.status == AssetStatus.Current for n in chain)


# ---------------------------------------------------------------------------
# Concurrent build tasks (async)
# ---------------------------------------------------------------------------

class TestConcurrentBuildAsync:
    async def test_diamond_dependency_builds_correctly(self):
        """Diamond: root -> (left, right) -> tip – tip must see both parents current."""
        root = MemoryAsset("root")
        left = MemoryAsset("left", dependencies=[root])
        right = MemoryAsset("right", dependencies=[root])
        tip = MemoryAsset("tip", dependencies=[left, right])
        g = AssetGraph("diamond")
        g.add_assets([tip])
        await g._build(assets=None)
        assert root.status == AssetStatus.Current
        assert left.status == AssetStatus.Current
        assert right.status == AssetStatus.Current
        assert tip.status == AssetStatus.Current

    async def test_independent_branches_both_built(self):
        a = MemoryAsset("a", build_delay=0.02)
        b = MemoryAsset("b", build_delay=0.02)
        g = AssetGraph("branches")
        g.add_assets([a, b])
        await g._build(assets=None)
        assert a.status == AssetStatus.Current
        assert b.status == AssetStatus.Current

    async def test_failed_parent_leaves_child_skipped(self):
        parent = MemoryAsset("fp", build_raises=True)
        child = MemoryAsset("fc", dependencies=[parent])
        g = AssetGraph("fail-chain")
        g.add_assets([child])
        await g._build(assets=None)
        assert parent.status == AssetStatus.Failed
        # child stays Unavailable / UpstreamStopped (not Current)
        assert child.status != AssetStatus.Current


# ---------------------------------------------------------------------------
# Resource lifecycle under concurrency
# ---------------------------------------------------------------------------

class TestResourceLifecycleAsync:
    async def test_resource_init_and_close_called_once_per_build(self):
        r = CountingResource()
        a = MemoryAsset("a", resources=[r])
        b = MemoryAsset("b", resources=[r])
        g = AssetGraph("res-test")
        g.add_assets([a, b])
        await g._build(assets=None)
        # Unique resources: init/close called exactly once each
        assert r.init_count == 1
        assert r.close_count == 1

    async def test_resources_cleaned_up_even_when_build_fails(self):
        r = CountingResource()
        a = MemoryAsset("a", build_raises=True, resources=[r])
        g = AssetGraph("res-fail")
        g.add_assets([a])
        await g._build(assets=None)
        assert r.close_count == 1

    async def test_failed_resource_init_still_calls_close(self):
        r_bad = CountingResource("bad", init_raises=True)
        r_good = CountingResource("good")
        a = MemoryAsset("a", resources=[r_bad, r_good])
        g = AssetGraph("res-init-fail")
        g.add_assets([a])
        await g._build(assets=None)
        # close should be called for all resources after a failed init
        assert r_bad.close_count == 1
        assert r_good.close_count == 1


# ---------------------------------------------------------------------------
# _get_message (thread interplay)
# ---------------------------------------------------------------------------

class TestGetMessage:
    def test_get_message_returns_none_on_timeout(self):
        g = AssetGraph("gm-test")
        result = g._get_message(timeout=0.05)
        assert result is None

    def test_get_message_returns_queued_item(self):
        g = AssetGraph("gm-test-2")
        g.message_queue.put({"type": "shutdown"})
        result = g._get_message(timeout=1)
        assert result == {"type": "shutdown"}

    def test_get_message_blocks_until_item_available(self):
        g = AssetGraph("gm-test-3")

        def delayed_put():
            time.sleep(0.05)
            g.message_queue.put({"type": "ping"})

        t = threading.Thread(target=delayed_put, daemon=True)
        t.start()
        result = g._get_message(timeout=2)
        t.join()
        assert result == {"type": "ping"}
