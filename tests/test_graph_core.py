"""Tests for AssetGraph core structure, listing, scheduling, and pause/unpause."""
import pytest

from fwirl.asset import AssetStatus
from fwirl.graph import AssetGraph
from tests.conftest import MemoryAsset, CountingResource


# ---------------------------------------------------------------------------
# add_assets
# ---------------------------------------------------------------------------

class TestAddAssets:
    def test_add_single_leaf(self, simple_graph):
        a = MemoryAsset("a")
        simple_graph.add_assets([a])
        assert simple_graph.graph.number_of_nodes() == 1

    def test_add_chain_via_leaf(self, simple_graph):
        a = MemoryAsset("a")
        b = MemoryAsset("b", dependencies=[a])
        c = MemoryAsset("c", dependencies=[b])
        simple_graph.add_assets([c])
        assert simple_graph.graph.number_of_nodes() == 3
        assert simple_graph.graph.number_of_edges() == 2

    def test_duplicate_asset_not_added_twice(self, simple_graph):
        a = MemoryAsset("a")
        simple_graph.add_assets([a])
        simple_graph.add_assets([a])
        assert simple_graph.graph.number_of_nodes() == 1

    def test_self_dependency_raises(self, simple_graph):
        a = MemoryAsset("self-dep")
        a.dependencies = [a]
        with pytest.raises(ValueError, match="Self-dependency"):
            simple_graph.add_assets([a])

    def test_non_async_build_raises(self, simple_graph):
        class SyncAsset(MemoryAsset):
            def build(self):  # type: ignore[override]
                pass

        with pytest.raises(ValueError):
            simple_graph.add_assets([SyncAsset("sync-build")])

    def test_non_async_timestamp_raises(self, simple_graph):
        class SyncAsset(MemoryAsset):
            def timestamp(self):  # type: ignore[override]
                return None

        with pytest.raises(ValueError):
            simple_graph.add_assets([SyncAsset("sync-ts")])

    def test_edges_created_correctly(self, simple_graph, linear_assets):
        a, b, c = linear_assets
        simple_graph.add_assets([c])
        assert simple_graph.graph.has_edge(a, b)
        assert simple_graph.graph.has_edge(b, c)
        assert not simple_graph.graph.has_edge(a, c)


# ---------------------------------------------------------------------------
# remove_assets
# ---------------------------------------------------------------------------

class TestRemoveAssets:
    def test_remove_leaf(self, populated_graph):
        g, a, b, c = populated_graph
        g.remove_assets([c])
        assert c not in g.graph
        assert b in g.graph

    def test_remove_root_removes_downstream(self, populated_graph):
        g, a, b, c = populated_graph
        g.remove_assets([a])
        # networkx removes the specified nodes; downstream survive unless removed too
        assert a not in g.graph

    def test_node_count_decreases(self, populated_graph):
        g, a, b, c = populated_graph
        before = g.graph.number_of_nodes()
        g.remove_assets([c])
        assert g.graph.number_of_nodes() == before - 1


# ---------------------------------------------------------------------------
# list_assets
# ---------------------------------------------------------------------------

class TestListAssets:
    def test_empty_graph(self, simple_graph):
        s = simple_graph.list_assets(display=False)
        assert "No assets" in s

    def test_lists_all_asset_keys(self, populated_graph):
        g, a, b, c = populated_graph
        s = g.list_assets(display=False)
        assert "A" in s
        assert "B" in s
        assert "C" in s

    def test_includes_status(self, populated_graph):
        g, a, b, c = populated_graph
        s = g.list_assets(display=False)
        assert "Unavailable" in s


# ---------------------------------------------------------------------------
# list_schedules
# ---------------------------------------------------------------------------

class TestListSchedules:
    def test_empty_schedules(self, simple_graph):
        s = simple_graph.list_schedules(display=False)
        assert "No schedules" in s

    def test_shows_schedule_name(self, simple_graph):
        simple_graph.schedule("hourly", "build", "0 * * * *")
        s = simple_graph.list_schedules(display=False)
        assert "hourly" in s


# ---------------------------------------------------------------------------
# list_jobs
# ---------------------------------------------------------------------------

class TestListJobs:
    def test_no_jobs_initially(self, simple_graph):
        s = simple_graph.list_jobs(display=False)
        assert "No running job" in s
        assert "No jobs" in s


# ---------------------------------------------------------------------------
# schedule / unschedule
# ---------------------------------------------------------------------------

class TestScheduleManagement:
    def test_add_build_schedule(self, simple_graph):
        simple_graph.schedule("s1", "build", "0 * * * *")
        assert "s1" in simple_graph.schedules

    def test_add_refresh_schedule(self, simple_graph):
        simple_graph.schedule("s2", "refresh", "*/5 * * * *")
        assert "s2" in simple_graph.schedules

    def test_duplicate_schedule_key_ignored(self, simple_graph, caplog):
        simple_graph.schedule("dup", "build", "0 * * * *")
        simple_graph.schedule("dup", "build", "0 * * * *")
        assert len([k for k in simple_graph.schedules if k == "dup"]) == 1

    def test_invalid_action_ignored(self, simple_graph, caplog):
        simple_graph.schedule("bad", "explode", "0 * * * *")
        assert "bad" not in simple_graph.schedules

    def test_unschedule_removes_schedule(self, simple_graph):
        simple_graph.schedule("s3", "build", "0 * * * *")
        simple_graph.unschedule("s3")
        assert "s3" not in simple_graph.schedules

    def test_unschedule_nonexistent_is_safe(self, simple_graph):
        simple_graph.unschedule("does-not-exist")  # should not raise

    def test_schedule_with_asset(self, populated_graph):
        g, a, b, c = populated_graph
        g.schedule("s-asset", "build", "0 * * * *", asset=a)
        assert "s-asset" in g.schedules

    def test_immediate_once_schedule(self, simple_graph):
        simple_graph.schedule("imm", "build", immediate_once=True)
        assert "imm" in simple_graph.schedules


# ---------------------------------------------------------------------------
# pause / unpause asset
# ---------------------------------------------------------------------------

class TestPauseUnpauseAsset:
    def test_pause_asset(self, populated_graph):
        g, a, b, c = populated_graph
        g.pause_asset(a)
        assert a.status == AssetStatus.Paused

    def test_unpause_asset_marks_stale(self, populated_graph):
        g, a, b, c = populated_graph
        g.pause_asset(a)
        g.unpause_asset(a)
        assert a.status == AssetStatus.Stale


# ---------------------------------------------------------------------------
# pause / unpause schedule
# ---------------------------------------------------------------------------

class TestPauseUnpauseSchedule:
    def test_pause_schedule(self, simple_graph):
        simple_graph.schedule("s", "build", "0 * * * *")
        simple_graph.pause_schedule("s")
        assert simple_graph.schedules["s"].is_paused()

    def test_unpause_schedule(self, simple_graph):
        simple_graph.schedule("s", "build", "0 * * * *")
        simple_graph.pause_schedule("s")
        simple_graph.unpause_schedule("s")
        assert not simple_graph.schedules["s"].is_paused()


# ---------------------------------------------------------------------------
# update_asset_properties
# ---------------------------------------------------------------------------

class TestUpdateAssetProperties:
    def test_updates_properties(self, populated_graph):
        g, a, b, c = populated_graph
        g.update_asset_properties("A", {"owner": "team"})
        assert a.properties == {"owner": "team"}

    def test_unknown_key_raises(self, simple_graph):
        with pytest.raises(ValueError, match="not found"):
            simple_graph.update_asset_properties("no-such-key", {})

    def test_non_dict_raises(self, populated_graph):
        g, a, b, c = populated_graph
        with pytest.raises(ValueError, match="dictionary"):
            g.update_asset_properties("A", "not-a-dict")


# ---------------------------------------------------------------------------
# _serialize_timestamp / _serialize_property_value
# ---------------------------------------------------------------------------

class TestSerializationHelpers:
    def test_serialize_unavailable_returns_none(self, simple_graph):
        assert simple_graph._serialize_timestamp(AssetStatus.Unavailable) is None

    def test_serialize_none_returns_none(self, simple_graph):
        assert simple_graph._serialize_timestamp(None) is None

    def test_serialize_pendulum_datetime(self, simple_graph):
        import pendulum as plm
        dt = plm.datetime(2024, 1, 15, 12, 0, 0, tz='UTC')
        s = simple_graph._serialize_timestamp(dt)
        assert "2024" in s

    def test_serialize_property_none(self, simple_graph):
        assert simple_graph._serialize_property_value(None) is None

    def test_serialize_property_bool(self, simple_graph):
        assert simple_graph._serialize_property_value(True) is True

    def test_serialize_property_int(self, simple_graph):
        assert simple_graph._serialize_property_value(42) == 42

    def test_serialize_property_float(self, simple_graph):
        assert simple_graph._serialize_property_value(3.14) == 3.14

    def test_serialize_property_str(self, simple_graph):
        assert simple_graph._serialize_property_value("hi") == "hi"

    def test_serialize_property_dict(self, simple_graph):
        result = simple_graph._serialize_property_value({"a": 1, "b": [1, 2]})
        assert result["a"] == 1
        assert result["b"] == [1, 2]

    def test_serialize_property_list(self, simple_graph):
        result = simple_graph._serialize_property_value([1, "x", None])
        assert result == [1, "x", None]

    def test_serialize_property_unknown_type(self, simple_graph):
        class Weird:
            def __str__(self):
                return "weird"
        result = simple_graph._serialize_property_value(Weird())
        assert result == "weird"


# ---------------------------------------------------------------------------
# _asset_by_key
# ---------------------------------------------------------------------------

class TestAssetByKey:
    def test_finds_existing_asset(self, populated_graph):
        g, a, b, c = populated_graph
        found = g._asset_by_key("B")
        assert found is b

    def test_returns_none_for_missing_key(self, populated_graph):
        g, *_ = populated_graph
        assert g._asset_by_key("nope") is None


# ---------------------------------------------------------------------------
# summarize / _collect_groups
# ---------------------------------------------------------------------------

class TestSummarize:
    def test_summarize_empty_graph(self, simple_graph):
        s = simple_graph.summarize(display=False)
        assert "Asset Graph Summary" in s
        assert "Assets: 0" in s

    def test_summarize_populated_graph(self, populated_graph):
        g, *_ = populated_graph
        s = g.summarize(display=False)
        assert "Assets: 3" in s
        assert "Edges: 2" in s

    def test_collect_groups_reserved_keyword_raises(self, simple_graph):
        a = MemoryAsset("a", group="__singletons__")
        simple_graph.add_assets([a])
        with pytest.raises(ValueError, match="reserved keyword"):
            simple_graph._collect_groups()

    def test_collect_groups_singletons(self, simple_graph):
        a = MemoryAsset("a")
        simple_graph.add_assets([a])
        groups = simple_graph._collect_groups()
        assert a in groups["__singletons__"]

    def test_collect_groups_with_group_and_subgroup(self, simple_graph):
        a = MemoryAsset("a", group="grp", subgroup="sg")
        simple_graph.add_assets([a])
        groups = simple_graph._collect_groups()
        assert "grp" in groups
        assert "sg" in groups["grp"]
        assert a in groups["grp"]["sg"]


# ---------------------------------------------------------------------------
# _preprocess_graph
# ---------------------------------------------------------------------------

class TestPreprocessGraph:
    def test_current_root_nodes_retained(self, simple_graph):
        a = MemoryAsset("a")
        a.status = AssetStatus.Current
        b = MemoryAsset("b", dependencies=[a])
        simple_graph.add_assets([b])
        processed = simple_graph._preprocess_graph()
        # root Current nodes should NOT be removed (they are roots)
        assert a in processed.nodes

    def test_non_current_nodes_retained(self, simple_graph):
        a = MemoryAsset("a")
        a.status = AssetStatus.Stale
        simple_graph.add_assets([a])
        processed = simple_graph._preprocess_graph()
        assert a in processed.nodes

    def test_original_graph_unchanged(self, populated_graph):
        g, a, b, c = populated_graph
        before_count = g.graph.number_of_nodes()
        g._preprocess_graph()
        assert g.graph.number_of_nodes() == before_count


# ---------------------------------------------------------------------------
# _asset_payload / _graph_payload (async)
# ---------------------------------------------------------------------------

class TestPayloads:
    async def test_asset_payload_keys(self, populated_graph):
        g, a, b, c = populated_graph
        payload = await g._asset_payload(a)
        expected_keys = {"key", "status", "message", "type", "group", "subgroup",
                         "allow_retry", "timestamp", "last_build_timestamp",
                         "parents", "children", "properties"}
        assert set(payload.keys()) == expected_keys

    async def test_asset_payload_values(self, populated_graph):
        g, a, b, c = populated_graph
        payload = await g._asset_payload(a)
        assert payload["key"] == "A"
        assert payload["status"] == "Unavailable"
        assert payload["parents"] == []
        assert "B" in payload["children"]

    async def test_graph_payload_structure(self, populated_graph):
        g, a, b, c = populated_graph
        payload = await g._graph_payload()
        assert "nodes" in payload
        assert "edges" in payload
        assert "collapse_candidates" in payload
        assert "summary" in payload
        assert payload["summary"]["asset_count"] == 3
        assert payload["summary"]["edge_count"] == 2

    async def test_graph_payload_collapse_candidates(self, simple_graph):
        # Two assets with same group/subgroup and same status
        import pendulum as plm
        ts = plm.now()
        a = MemoryAsset("a1", group="g", subgroup="sg", initial_ts=ts)
        a.status = AssetStatus.Current
        b = MemoryAsset("b1", group="g", subgroup="sg", initial_ts=ts)
        b.status = AssetStatus.Current
        simple_graph.add_assets([a, b])
        payload = await simple_graph._graph_payload()
        assert len(payload["collapse_candidates"]) >= 1

    async def test_graph_payload_no_collapse_for_mixed_status(self, simple_graph):
        import pendulum as plm
        a = MemoryAsset("a2", group="g2", subgroup="sg2")
        a.status = AssetStatus.Current
        b = MemoryAsset("b2", group="g2", subgroup="sg2")
        b.status = AssetStatus.Stale
        simple_graph.add_assets([a, b])
        payload = await simple_graph._graph_payload()
        # Mixed statuses => no collapse candidates for this group
        assert all(
            c["group"] != "g2" for c in payload["collapse_candidates"]
        )
