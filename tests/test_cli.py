"""Tests for fwirl.cli – all Click command entry-points.

All API functions and server helpers are mocked so that tests run in
isolation without RabbitMQ or a live web server.
"""
from unittest.mock import patch, MagicMock

import pytest
from click.testing import CliRunner

from fwirl.cli import cli

# Patch targets – the names as imported into the cli module
_API = "fwirl.cli"


@pytest.fixture
def runner():
    return CliRunner()


# ---------------------------------------------------------------------------
# list-graphs
# ---------------------------------------------------------------------------

class TestListGraphs:
    def test_invokes_api(self, runner):
        with patch(f"{_API}.api_list_graphs") as mock:
            result = runner.invoke(cli, ["list-graphs"])
        assert result.exit_code == 0
        mock.assert_called_once()

    def test_no_extra_args(self, runner):
        with patch(f"{_API}.api_list_graphs"):
            result = runner.invoke(cli, ["list-graphs"])
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# summarize
# ---------------------------------------------------------------------------

class TestSummarize:
    def test_passes_graph_key(self, runner):
        with patch(f"{_API}.api_summarize") as mock:
            runner.invoke(cli, ["summarize", "my-graph"])
        mock.assert_called_once()
        assert mock.call_args[0][0] == "my-graph"

    def test_custom_rabbit_url(self, runner):
        with patch(f"{_API}.api_summarize") as mock:
            runner.invoke(cli, ["summarize", "g", "--rabbit_url", "amqp://host/"])
        # rabbit_url is passed as a positional argument by the CLI
        assert mock.call_args[0][1] == "amqp://host/"


# ---------------------------------------------------------------------------
# shutdown
# ---------------------------------------------------------------------------

class TestShutdown:
    def test_passes_graph_key(self, runner):
        with patch(f"{_API}.api_shutdown") as mock:
            runner.invoke(cli, ["shutdown", "g"])
        mock.assert_called_once()
        assert mock.call_args[0][0] == "g"


# ---------------------------------------------------------------------------
# ls
# ---------------------------------------------------------------------------

class TestLs:
    def test_no_flags(self, runner):
        with patch(f"{_API}.api_ls") as mock:
            runner.invoke(cli, ["ls", "g"])
        args, kwargs = mock.call_args
        assert args[0] == "g"
        assert args[1] is False  # assets
        assert args[2] is False  # schedules
        assert args[3] is False  # jobs

    def test_all_flags(self, runner):
        with patch(f"{_API}.api_ls") as mock:
            runner.invoke(cli, ["ls", "g", "--assets", "--schedules", "--jobs"])
        args, kwargs = mock.call_args
        assert args[1] is True
        assert args[2] is True
        assert args[3] is True


# ---------------------------------------------------------------------------
# refresh
# ---------------------------------------------------------------------------

class TestRefresh:
    def test_refresh_all(self, runner):
        with patch(f"{_API}.api_refresh") as mock:
            runner.invoke(cli, ["refresh", "g"])
        args, kwargs = mock.call_args
        assert args[0] == "g"
        assert args[1] is None  # no specific asset

    def test_refresh_specific_asset(self, runner):
        with patch(f"{_API}.api_refresh") as mock:
            runner.invoke(cli, ["refresh", "g", "--asset", "my-asset"])
        assert mock.call_args[0][1] == "my-asset"


# ---------------------------------------------------------------------------
# build
# ---------------------------------------------------------------------------

class TestBuild:
    def test_build_all(self, runner):
        with patch(f"{_API}.api_build") as mock:
            runner.invoke(cli, ["build", "g"])
        assert mock.call_args[0][1] is None

    def test_build_specific_asset(self, runner):
        with patch(f"{_API}.api_build") as mock:
            runner.invoke(cli, ["build", "g", "--asset", "x"])
        assert mock.call_args[0][1] == "x"


# ---------------------------------------------------------------------------
# pause / unpause
# ---------------------------------------------------------------------------

class TestPauseUnpause:
    def test_pause_passes_graph_and_key(self, runner):
        with patch(f"{_API}.api_pause") as mock:
            runner.invoke(cli, ["pause", "g", "some-key"])
        assert mock.call_args[0][0] == "g"
        assert mock.call_args[0][1] == "some-key"

    def test_unpause_passes_graph_and_key(self, runner):
        with patch(f"{_API}.api_unpause") as mock:
            runner.invoke(cli, ["unpause", "g", "some-key"])
        assert mock.call_args[0][0] == "g"
        assert mock.call_args[0][1] == "some-key"


# ---------------------------------------------------------------------------
# schedule / unschedule
# ---------------------------------------------------------------------------

class TestScheduleCLI:
    def test_schedule_all_args(self, runner):
        with patch(f"{_API}.api_schedule") as mock:
            runner.invoke(cli, ["schedule", "g", "s1", "build", "0 * * * *"])
        args = mock.call_args[0]
        assert args[0] == "g"
        assert args[1] == "s1"
        assert args[2] == "build"
        assert args[3] == "0 * * * *"

    def test_schedule_with_asset(self, runner):
        with patch(f"{_API}.api_schedule") as mock:
            runner.invoke(cli, ["schedule", "g", "s1", "build", "0 * * * *",
                                "--asset", "my-asset"])
        assert mock.call_args[0][4] == "my-asset"

    def test_unschedule(self, runner):
        with patch(f"{_API}.api_unschedule") as mock:
            runner.invoke(cli, ["unschedule", "g", "s1"])
        assert mock.call_args[0][0] == "g"
        assert mock.call_args[0][1] == "s1"


# ---------------------------------------------------------------------------
# webserver subcommands
# ---------------------------------------------------------------------------

class TestWebserverCLI:
    def test_webserver_start(self, runner):
        with patch(f"{_API}.start_webserver") as mock:
            result = runner.invoke(cli, ["webserver", "start"])
        assert result.exit_code == 0
        mock.assert_called_once()

    def test_webserver_stop(self, runner):
        with patch(f"{_API}.stop_webserver") as mock:
            result = runner.invoke(cli, ["webserver", "stop"])
        assert result.exit_code == 0
        mock.assert_called_once()


# ---------------------------------------------------------------------------
# Help / bad commands
# ---------------------------------------------------------------------------

class TestCliHelp:
    def test_help_exits_zero(self, runner):
        result = runner.invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "Usage" in result.output

    def test_unknown_command_fails(self, runner):
        result = runner.invoke(cli, ["no-such-command"])
        assert result.exit_code != 0

    def test_missing_required_arg_fails(self, runner):
        result = runner.invoke(cli, ["summarize"])
        assert result.exit_code != 0
