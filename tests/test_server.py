"""Tests for fwirl.server – web server helpers and dashboard HTML generation.

All RabbitMQ I/O and process management is mocked.
"""
import html as _html
import json
from unittest.mock import patch, MagicMock

import pytest

from fwirl.server import dashboard_html, request_graph, getgraph, __SERVERPORT__

PUBLISH = "fwirl.server.publish_msg"
GET_MSG = "fwirl.server.get_msg"


def _make_get_msg_side_effect(response_body):
    def _effect(key, queue, url):
        queue.put({"response": response_body})
    return _effect


# ---------------------------------------------------------------------------
# dashboard_html
# ---------------------------------------------------------------------------

class TestDashboardHtml:
    def test_returns_string(self):
        html = dashboard_html("test-graph")
        assert isinstance(html, str)

    def test_contains_graph_key(self):
        html = dashboard_html("my-graph")
        assert "my-graph" in html

    def test_html_escapes_graph_key(self):
        html = dashboard_html('<script>alert(1)</script>')
        # The raw script tag should NOT appear anywhere in the HTML
        assert "<script>alert(1)</script>" not in html
        # The HTML-escaped form should be present in the HTML element
        assert "&lt;script&gt;" in html

    def test_js_safe_graph_key_encoding(self):
        # Characters dangerous in <script> contexts must be encoded in the
        # localStorage.setItem call that embeds the graph key.
        import re
        for dangerous_char in ("<", ">", "&"):
            html_out = dashboard_html(dangerous_char)
            # Find the localStorage line specifically
            matches = re.findall(
                r'localStorage\.setItem\("fwirl:lastGraphKey",\s*(.+?)\);',
                html_out
            )
            assert matches, "localStorage.setItem line not found"
            for m in matches:
                assert dangerous_char not in m, (
                    f"Dangerous char '{dangerous_char}' found unescaped in JS value: {m!r}"
                )

    def test_is_valid_html_document(self):
        html = dashboard_html("g")
        assert "<!doctype html>" in html.lower()
        assert "<html" in html
        assert "</html>" in html

    def test_contains_required_js_libs(self):
        html = dashboard_html("g")
        assert "vis-network" in html
        assert "tabulator" in html

    def test_contains_fwirl_dashboard_title(self):
        html = dashboard_html("g")
        assert "fwirl" in html.lower()

    def test_server_port_constant(self):
        assert isinstance(__SERVERPORT__, int)
        assert __SERVERPORT__ > 0

    def test_different_keys_produce_different_html(self):
        h1 = dashboard_html("graph-a")
        h2 = dashboard_html("graph-b")
        assert h1 != h2


# ---------------------------------------------------------------------------
# request_graph
# ---------------------------------------------------------------------------

class TestRequestGraph:
    def test_no_response_when_wait_false(self):
        with patch(PUBLISH) as mock_pub:
            result = request_graph("g", {"type": "shutdown"}, wait_for_response=False)
        assert result is None
        mock_pub.assert_called_once()

    def test_publishes_message_with_resp_queue(self):
        resp = {"data": "value"}
        with patch(PUBLISH) as mock_pub, \
             patch(GET_MSG, side_effect=_make_get_msg_side_effect(resp)):
            result = request_graph("g", {"type": "graph_snapshot"})
        body = mock_pub.call_args[0][1]
        assert "resp_queue" in body
        assert result == resp

    def test_graph_key_used_as_routing_key(self):
        resp = {}
        with patch(PUBLISH) as mock_pub, \
             patch(GET_MSG, side_effect=_make_get_msg_side_effect(resp)):
            request_graph("my-graph", {"type": "summarize"})
        assert mock_pub.call_args[0][0] == "my-graph"


# ---------------------------------------------------------------------------
# getgraph
# ---------------------------------------------------------------------------

class TestGetGraph:
    def test_sends_graph_message_type(self):
        resp = b"<svg/>"
        with patch(PUBLISH) as mock_pub, \
             patch(GET_MSG, side_effect=_make_get_msg_side_effect(resp)):
            result = getgraph("g")
        body = mock_pub.call_args[0][1]
        assert body["type"] == "graph"
        assert result == resp


# ---------------------------------------------------------------------------
# bulk pause / unpause routes
# ---------------------------------------------------------------------------

class TestBulkPauseRoutes:
    async def _post(self, path, body):
        from aiohttp.test_utils import TestClient, TestServer
        from fwirl.server import aiohttp_server

        runner = aiohttp_server()
        async with TestClient(TestServer(runner.app)) as client:
            resp = await client.post(path, json=body)
            return resp.status, await resp.json()

    @pytest.mark.parametrize("action", ["pause", "unpause"])
    async def test_sends_one_message_per_asset(self, action):
        with patch("fwirl.server.request_graph") as rg:
            status, payload = await self._post(f"/api/graphs/g/{action}", {"asset_keys": ["a", "b"]})
        assert status == 200
        assert payload == {"ok": True, "count": 2}
        assert [c.args for c in rg.call_args_list] == [
            ("g", {"type": action, "key": "a"}),
            ("g", {"type": action, "key": "b"}),
        ]
        assert all(c.kwargs == {"wait_for_response": False} for c in rg.call_args_list)

    @pytest.mark.parametrize("route,value", [("allow-retry", True), ("disallow-retry", False)])
    async def test_retry_routes_send_set_allow_retry(self, route, value):
        with patch("fwirl.server.request_graph") as rg:
            status, payload = await self._post(f"/api/graphs/g/{route}", {"asset_keys": ["a", "b"]})
        assert status == 200
        assert payload == {"ok": True, "count": 2}
        assert [c.args for c in rg.call_args_list] == [
            ("g", {"type": "set_allow_retry", "key": "a", "value": value}),
            ("g", {"type": "set_allow_retry", "key": "b", "value": value}),
        ]

    @pytest.mark.parametrize("body", [{}, {"asset_keys": "a"}, {"asset_keys": [1]}, []])
    async def test_rejects_invalid_body(self, body):
        with patch("fwirl.server.request_graph") as rg:
            status, payload = await self._post("/api/graphs/g/pause", body)
        assert status == 400
        assert "error" in payload
        rg.assert_not_called()

    async def test_empty_list_is_a_noop(self):
        with patch("fwirl.server.request_graph") as rg:
            status, payload = await self._post("/api/graphs/g/pause", {"asset_keys": []})
        assert status == 200
        assert payload["count"] == 0
        rg.assert_not_called()


class TestDashboardPauseButtons:
    def test_contains_pause_and_unpause_buttons(self):
        html = dashboard_html("g")
        assert 'id="pause-btn"' in html
        assert 'id="unpause-btn"' in html

    def test_contains_retry_buttons(self):
        html = dashboard_html("g")
        assert 'id="allow-retry-btn"' in html
        assert 'id="disallow-retry-btn"' in html
