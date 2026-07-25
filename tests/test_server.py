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
