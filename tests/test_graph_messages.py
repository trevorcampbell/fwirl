"""Tests for AssetGraph._process_message – all message type handlers."""
import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

import pendulum as plm
import pytest

from fwirl.asset import AssetStatus
from fwirl.graph import AssetGraph, ShutdownSignal
from tests.conftest import MemoryAsset


def _graph_with(*assets):
    g = AssetGraph("msg-test")
    g.add_assets(list(assets))
    return g


# We patch publish_msg so that tests never attempt a real RabbitMQ connection.
PUBLISH_PATH = "fwirl.graph.publish_msg"


# ---------------------------------------------------------------------------
# summarize message
# ---------------------------------------------------------------------------

class TestSummarizeMessage:
    async def test_summarize_publishes_response(self):
        g = AssetGraph("g")
        msg = {"type": "summarize", "resp_queue": "q-summ"}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        mock_pub.assert_called_once()
        args = mock_pub.call_args[0]
        assert args[0] == "q-summ"
        assert "response" in args[1]


# ---------------------------------------------------------------------------
# ls message
# ---------------------------------------------------------------------------

class TestLsMessage:
    async def test_ls_assets_only(self):
        a = MemoryAsset("a")
        g = _graph_with(a)
        msg = {"type": "ls", "resp_queue": "q-ls", "assets": True, "schedules": False, "jobs": False}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        resp = mock_pub.call_args[0][1]["response"]
        assert "a" in resp

    async def test_ls_schedules_only(self):
        g = AssetGraph("g")
        g.schedule("s1", "build", "0 * * * *")
        msg = {"type": "ls", "resp_queue": "q-ls2", "assets": False, "schedules": True, "jobs": False}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        resp = mock_pub.call_args[0][1]["response"]
        assert "s1" in resp

    async def test_ls_empty_response(self):
        g = AssetGraph("g")
        msg = {"type": "ls", "resp_queue": "q-ls3", "assets": False, "schedules": False, "jobs": False}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        resp = mock_pub.call_args[0][1]["response"]
        assert resp == ""


# ---------------------------------------------------------------------------
# pause / unpause message
# ---------------------------------------------------------------------------

class TestPauseUnpauseMessage:
    async def test_pause_asset_by_key(self):
        a = MemoryAsset("asset-x")
        g = _graph_with(a)
        msg = {"type": "pause", "key": "asset-x"}
        await g._process_message(msg)
        assert a.status == AssetStatus.Paused

    async def test_unpause_asset_by_key(self):
        a = MemoryAsset("asset-y")
        a.status = AssetStatus.Paused
        g = _graph_with(a)
        msg = {"type": "unpause", "key": "asset-y"}
        await g._process_message(msg)
        assert a.status == AssetStatus.Stale

    async def test_pause_schedule_by_key(self):
        g = AssetGraph("g")
        g.schedule("sch1", "build", "0 * * * *")
        msg = {"type": "pause", "key": "sch1"}
        await g._process_message(msg)
        assert g.schedules["sch1"].is_paused()

    async def test_unpause_schedule_by_key(self):
        g = AssetGraph("g")
        g.schedule("sch2", "build", "0 * * * *")
        g.schedules["sch2"].pause()
        msg = {"type": "unpause", "key": "sch2"}
        await g._process_message(msg)
        assert not g.schedules["sch2"].is_paused()

    async def test_pause_nonexistent_key_is_safe(self):
        g = AssetGraph("g")
        msg = {"type": "pause", "key": "no-such-key"}
        await g._process_message(msg)  # should not raise


# ---------------------------------------------------------------------------
# schedule / unschedule message
# ---------------------------------------------------------------------------

class TestScheduleUnscheduleMessage:
    async def test_schedule_message_adds_schedule(self):
        g = AssetGraph("g")
        msg = {"type": "schedule", "schedule_key": "new-s",
               "action": "build", "cron_string": "0 * * * *", "asset_key": None}
        await g._process_message(msg)
        assert "new-s" in g.schedules

    async def test_unschedule_message_removes_schedule(self):
        g = AssetGraph("g")
        g.schedule("to-remove", "build", "0 * * * *")
        msg = {"type": "unschedule", "schedule_key": "to-remove"}
        await g._process_message(msg)
        assert "to-remove" not in g.schedules


# ---------------------------------------------------------------------------
# build / refresh message (add immediate-once schedule)
# ---------------------------------------------------------------------------

class TestBuildRefreshMessage:
    async def test_build_message_adds_immediate_schedule(self):
        g = AssetGraph("g")
        msg = {"type": "build", "asset_key": None}
        await g._process_message(msg)
        # One immediate-once build schedule should have been added
        keys = list(g.schedules.keys())
        assert any("immediate-build" in k for k in keys)

    async def test_refresh_message_adds_immediate_schedule(self):
        g = AssetGraph("g")
        msg = {"type": "refresh", "asset_key": None}
        await g._process_message(msg)
        keys = list(g.schedules.keys())
        assert any("immediate-refresh" in k for k in keys)


# ---------------------------------------------------------------------------
# shutdown message
# ---------------------------------------------------------------------------

class TestShutdownMessage:
    async def test_shutdown_raises_shutdown_signal(self):
        g = AssetGraph("g")
        with pytest.raises(ShutdownSignal):
            await g._process_message({"type": "shutdown"})


# ---------------------------------------------------------------------------
# graph_snapshot message
# ---------------------------------------------------------------------------

class TestGraphSnapshotMessage:
    async def test_graph_snapshot_publishes_payload(self):
        a = MemoryAsset("a")
        g = _graph_with(a)
        msg = {"type": "graph_snapshot", "resp_queue": "q-snap"}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        payload = mock_pub.call_args[0][1]["response"]
        assert "nodes" in payload
        assert "edges" in payload


# ---------------------------------------------------------------------------
# asset_detail message
# ---------------------------------------------------------------------------

class TestAssetDetailMessage:
    async def test_asset_detail_returns_payload(self):
        a = MemoryAsset("detail-asset")
        g = _graph_with(a)
        msg = {"type": "asset_detail", "asset_key": "detail-asset", "resp_queue": "q-det"}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        payload = mock_pub.call_args[0][1]["response"]
        assert payload["key"] == "detail-asset"

    async def test_asset_detail_unknown_key_returns_none(self):
        g = AssetGraph("g")
        msg = {"type": "asset_detail", "asset_key": "nope", "resp_queue": "q-det2"}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        assert mock_pub.call_args[0][1]["response"] is None


# ---------------------------------------------------------------------------
# update_asset_properties message
# ---------------------------------------------------------------------------

class TestUpdateAssetPropertiesMessage:
    async def test_updates_properties(self):
        a = MemoryAsset("prop-asset")
        g = _graph_with(a)
        msg = {"type": "update_asset_properties", "asset_key": "prop-asset",
               "properties": {"k": "v"}, "resp_queue": "q-prop"}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        assert a.properties == {"k": "v"}
        payload = mock_pub.call_args[0][1]["response"]
        assert payload["ok"] is True

    async def test_unknown_key_returns_error(self):
        g = AssetGraph("g")
        msg = {"type": "update_asset_properties", "asset_key": "nope",
               "properties": {}, "resp_queue": "q-prop2"}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        payload = mock_pub.call_args[0][1]["response"]
        assert payload["ok"] is False
        assert "error" in payload

    async def test_no_resp_queue_does_not_publish(self):
        a = MemoryAsset("silent-asset")
        g = _graph_with(a)
        msg = {"type": "update_asset_properties", "asset_key": "silent-asset",
               "properties": {"x": 1}}
        with patch(PUBLISH_PATH) as mock_pub:
            await g._process_message(msg)
        mock_pub.assert_not_called()


# ---------------------------------------------------------------------------
# None message (timeout)
# ---------------------------------------------------------------------------

class TestNoneMessage:
    async def test_none_message_is_ignored(self):
        g = AssetGraph("g")
        await g._process_message(None)  # must not raise
