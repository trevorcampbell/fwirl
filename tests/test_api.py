"""Tests for fwirl.api – all public API functions.

All RabbitMQ I/O is mocked so the tests run without a broker.
"""
from unittest.mock import MagicMock, patch, call
from queue import Queue

import pytest

from fwirl import api

PUBLISH = "fwirl.api.publish_msg"
GET_MSG = "fwirl.api.get_msg"
LIST_GRAPHS = "fwirl.api.list_running_graphs"


def _make_get_msg_side_effect(response_body):
    """Return a side_effect for get_msg that puts *response_body* into the queue."""
    def _effect(key, queue, url):
        queue.put(response_body)
    return _effect


# ---------------------------------------------------------------------------
# list_graphs
# ---------------------------------------------------------------------------

class TestListGraphs:
    def test_no_graphs(self, capsys):
        with patch(LIST_GRAPHS, return_value=[]):
            result = api.list_graphs()
        out = capsys.readouterr().out
        assert "No running graphs" in out
        assert result == []

    def test_with_graphs(self, capsys):
        with patch(LIST_GRAPHS, return_value=["graph-a", "graph-b"]):
            result = api.list_graphs()
        out = capsys.readouterr().out
        assert "graph-a" in out
        assert "graph-b" in out
        assert result == ["graph-a", "graph-b"]


# ---------------------------------------------------------------------------
# summarize
# ---------------------------------------------------------------------------

class TestSummarize:
    def test_publishes_correct_message(self, capsys):
        resp = {"type": "response", "response": "summary text"}
        with patch(PUBLISH) as mock_pub, \
             patch(GET_MSG, side_effect=_make_get_msg_side_effect(resp)):
            api.summarize("my-graph")
        # First positional arg is the graph key
        assert mock_pub.call_args[0][0] == "my-graph"
        body = mock_pub.call_args[0][1]
        assert body["type"] == "summarize"

    def test_prints_response(self, capsys):
        resp = {"type": "response", "response": "the summary"}
        with patch(PUBLISH), \
             patch(GET_MSG, side_effect=_make_get_msg_side_effect(resp)):
            api.summarize("g")
        assert "the summary" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# ls
# ---------------------------------------------------------------------------

class TestLs:
    def test_publishes_ls_message(self):
        resp = {"type": "response", "response": "ls output"}
        with patch(PUBLISH) as mock_pub, \
             patch(GET_MSG, side_effect=_make_get_msg_side_effect(resp)):
            api.ls("g", assets=True, schedules=False, jobs=True)
        body = mock_pub.call_args[0][1]
        assert body["type"] == "ls"
        assert body["assets"] is True
        assert body["schedules"] is False
        assert body["jobs"] is True

    def test_prints_response(self, capsys):
        resp = {"type": "response", "response": "ls result"}
        with patch(PUBLISH), \
             patch(GET_MSG, side_effect=_make_get_msg_side_effect(resp)):
            api.ls("g")
        assert "ls result" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------

class TestShutdown:
    def test_publishes_shutdown(self):
        with patch(PUBLISH) as mock_pub:
            api.shutdown("my-graph")
        body = mock_pub.call_args[0][1]
        assert body["type"] == "shutdown"
        assert mock_pub.call_args[0][0] == "my-graph"


# ---------------------------------------------------------------------------
# refresh
# ---------------------------------------------------------------------------

class TestRefresh:
    def test_publishes_refresh_all(self):
        with patch(PUBLISH) as mock_pub:
            api.refresh("g")
        body = mock_pub.call_args[0][1]
        assert body["type"] == "refresh"
        assert body["asset_key"] is None

    def test_publishes_refresh_specific_asset(self):
        with patch(PUBLISH) as mock_pub:
            api.refresh("g", asset_key="my-asset")
        body = mock_pub.call_args[0][1]
        assert body["asset_key"] == "my-asset"


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

class TestBuild:
    def test_publishes_build_all(self):
        with patch(PUBLISH) as mock_pub:
            api.build("g")
        body = mock_pub.call_args[0][1]
        assert body["type"] == "build"
        assert body["asset_key"] is None

    def test_publishes_build_specific_asset(self):
        with patch(PUBLISH) as mock_pub:
            api.build("g", asset_key="x")
        body = mock_pub.call_args[0][1]
        assert body["asset_key"] == "x"


# ---------------------------------------------------------------------------
# pause / unpause
# ---------------------------------------------------------------------------

class TestPauseUnpause:
    def test_pause_publishes_pause(self):
        with patch(PUBLISH) as mock_pub:
            api.pause("g", key="some-key")
        body = mock_pub.call_args[0][1]
        assert body["type"] == "pause"
        assert body["key"] == "some-key"

    def test_unpause_publishes_unpause(self):
        with patch(PUBLISH) as mock_pub:
            api.unpause("g", key="some-key")
        body = mock_pub.call_args[0][1]
        assert body["type"] == "unpause"
        assert body["key"] == "some-key"


# ---------------------------------------------------------------------------
# schedule / unschedule
# ---------------------------------------------------------------------------

class TestScheduleUnschedule:
    def test_schedule_publishes_correct_body(self):
        with patch(PUBLISH) as mock_pub:
            api.schedule("g", "s1", "build", "0 * * * *", asset_key="a")
        body = mock_pub.call_args[0][1]
        assert body["type"] == "schedule"
        assert body["schedule_key"] == "s1"
        assert body["action"] == "build"
        assert body["cron_string"] == "0 * * * *"
        assert body["asset_key"] == "a"

    def test_unschedule_publishes_correct_body(self):
        with patch(PUBLISH) as mock_pub:
            api.unschedule("g", "s1")
        body = mock_pub.call_args[0][1]
        assert body["type"] == "unschedule"
        assert body["schedule_key"] == "s1"
