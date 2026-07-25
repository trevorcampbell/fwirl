"""Tests for fwirl.registry – process registration and discovery."""
import json
import os
import tempfile

import psutil
import pytest

from fwirl.registry import (
    __GRAPH_PID_PREFIX__,
    list_running_graphs,
    register_graph_process,
    unregister_graph_process,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cleanup_all_test_graphs():
    """Remove any lingering temp files from previous test runs."""
    for f in _find_test_temp_files():
        try:
            os.unlink(f)
        except FileNotFoundError:
            pass


def _find_test_temp_files():
    import glob as _glob
    return _glob.glob(
        os.path.join(tempfile.gettempdir(), f"{__GRAPH_PID_PREFIX__}*")
    )


@pytest.fixture(autouse=True)
def clean_registry():
    """Ensure the registry is empty before and after every test."""
    _cleanup_all_test_graphs()
    yield
    _cleanup_all_test_graphs()


# ---------------------------------------------------------------------------
# list_running_graphs
# ---------------------------------------------------------------------------

class TestListRunningGraphs:
    def test_returns_empty_when_no_graphs_registered(self):
        assert list_running_graphs() == []

    def test_returns_sorted_list(self):
        f1 = register_graph_process("zebra")
        f2 = register_graph_process("apple")
        try:
            graphs = list_running_graphs()
            assert graphs == ["apple", "zebra"]
        finally:
            unregister_graph_process(f1)
            unregister_graph_process(f2)

    def test_stale_pid_file_is_ignored(self):
        # Write a pid-file with a non-existent PID
        info = {"graph_key": "ghost", "pid": 99999999, "create_time": 0.0}
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, prefix=__GRAPH_PID_PREFIX__, dir=tempfile.gettempdir()
        ) as f:
            json.dump(info, f)
            fname = f.name
        graphs = list_running_graphs()
        # The stale file should have been cleaned up
        assert "ghost" not in graphs
        assert not os.path.exists(fname)

    def test_corrupt_pid_file_is_ignored(self):
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, prefix=__GRAPH_PID_PREFIX__, dir=tempfile.gettempdir()
        ) as f:
            f.write("not valid json {{{")
            fname = f.name
        graphs = list_running_graphs()
        assert not os.path.exists(fname)

    def test_pid_file_with_wrong_create_time_is_ignored(self):
        pid = os.getpid()
        info = {"graph_key": "wrong-time", "pid": pid, "create_time": 0.0}
        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, prefix=__GRAPH_PID_PREFIX__, dir=tempfile.gettempdir()
        ) as f:
            json.dump(info, f)
            fname = f.name
        graphs = list_running_graphs()
        assert "wrong-time" not in graphs
        assert not os.path.exists(fname)


# ---------------------------------------------------------------------------
# register_graph_process
# ---------------------------------------------------------------------------

class TestRegisterGraphProcess:
    def test_returns_file_path(self):
        f = register_graph_process("g1")
        try:
            assert os.path.exists(f)
            assert __GRAPH_PID_PREFIX__ in os.path.basename(f)
        finally:
            unregister_graph_process(f)

    def test_registered_graph_appears_in_list(self):
        f = register_graph_process("my-graph")
        try:
            assert "my-graph" in list_running_graphs()
        finally:
            unregister_graph_process(f)

    def test_duplicate_key_raises(self):
        f = register_graph_process("dup")
        try:
            with pytest.raises(ValueError, match="already exists"):
                register_graph_process("dup")
        finally:
            unregister_graph_process(f)

    def test_multiple_graphs_coexist(self):
        f1 = register_graph_process("g-alpha")
        f2 = register_graph_process("g-beta")
        try:
            graphs = list_running_graphs()
            assert "g-alpha" in graphs
            assert "g-beta" in graphs
        finally:
            unregister_graph_process(f1)
            unregister_graph_process(f2)

    def test_custom_pid_stored_in_file(self):
        pid = os.getpid()
        f = register_graph_process("pid-check", pid=pid)
        try:
            with open(f) as fh:
                data = json.load(fh)
            assert data["pid"] == pid
            assert data["graph_key"] == "pid-check"
        finally:
            unregister_graph_process(f)


# ---------------------------------------------------------------------------
# unregister_graph_process
# ---------------------------------------------------------------------------

class TestUnregisterGraphProcess:
    def test_removes_file(self):
        f = register_graph_process("rem")
        assert os.path.exists(f)
        unregister_graph_process(f)
        assert not os.path.exists(f)

    def test_graph_disappears_from_list_after_unregister(self):
        f = register_graph_process("gone")
        unregister_graph_process(f)
        assert "gone" not in list_running_graphs()

    def test_unregister_none_is_safe(self):
        unregister_graph_process(None)  # should not raise

    def test_unregister_nonexistent_path_is_safe(self):
        unregister_graph_process("/tmp/does_not_exist_fwirl_test_xyz")
