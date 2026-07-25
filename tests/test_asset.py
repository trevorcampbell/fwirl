"""Tests for fwirl.asset – Asset, ExternalAsset, and AssetStatus."""
import asyncio

import pendulum as plm
import pytest

from fwirl.asset import Asset, AssetStatus, ExternalAsset
from tests.conftest import MemoryAsset, SimpleExternalAsset


# ---------------------------------------------------------------------------
# AssetStatus
# ---------------------------------------------------------------------------

class TestAssetStatus:
    def test_all_values_present(self):
        names = {s.name for s in AssetStatus}
        assert names == {"Current", "Stale", "Building", "Paused",
                         "UpstreamStopped", "Unavailable", "Failed"}

    def test_integer_values(self):
        assert AssetStatus.Current.value == 0
        assert AssetStatus.Stale.value == 1
        assert AssetStatus.Building.value == 2
        assert AssetStatus.Paused.value == 3
        assert AssetStatus.UpstreamStopped.value == 4
        assert AssetStatus.Unavailable.value == 5
        assert AssetStatus.Failed.value == 6

    def test_enum_comparison(self):
        assert AssetStatus.Current != AssetStatus.Stale
        assert AssetStatus.Current == AssetStatus.Current


# ---------------------------------------------------------------------------
# Asset (via MemoryAsset)
# ---------------------------------------------------------------------------

class TestAsset:
    def test_init_defaults(self):
        a = MemoryAsset("my-key")
        assert a.key == "my-key"
        assert a.dependencies == []
        assert a.resources == []
        assert a.status == AssetStatus.Unavailable
        assert a.message == ""
        assert a.group is None
        assert a.subgroup is None
        assert a.allow_retry is True
        assert a.properties == {}

    def test_init_with_all_kwargs(self):
        dep = MemoryAsset("dep")
        a = MemoryAsset(
            "asset",
            dependencies=[dep],
            group="g",
            subgroup="sg",
            allow_retry=False,
            properties={"k": "v"},
        )
        assert a.dependencies == [dep]
        assert a.group == "g"
        assert a.subgroup == "sg"
        assert a.allow_retry is False
        assert a.properties == {"k": "v"}

    def test_hash_based_on_key(self):
        a = MemoryAsset("x")
        b = MemoryAsset("x")
        c = MemoryAsset("y")
        assert hash(a) == hash(b)
        assert hash(a) != hash(c)

    def test_equality_based_on_hash(self):
        a = MemoryAsset("x")
        b = MemoryAsset("x")
        c = MemoryAsset("y")
        assert a == b
        assert a != c

    def test_repr_is_key(self):
        a = MemoryAsset("my-asset")
        assert repr(a) == "my-asset"

    def test_get_key(self):
        a = MemoryAsset("k")
        assert a.get_key() == "k"

    def test_properties_copy_on_init(self):
        props = {"a": 1}
        a = MemoryAsset("x", properties=props)
        props["b"] = 2
        assert "b" not in a.properties

    async def test_timestamp_unavailable_by_default(self):
        a = MemoryAsset("x")
        assert await a.timestamp() is AssetStatus.Unavailable

    async def test_build_sets_timestamp(self):
        a = MemoryAsset("x")
        before = plm.now()
        await a.build()
        ts = await a.timestamp()
        assert isinstance(ts, plm.DateTime)
        assert ts >= before

    def test_base_asset_can_be_instantiated(self):
        # Asset does not inherit from ABC, so instantiation succeeds;
        # subclasses are expected to implement timestamp() and build().
        a = Asset("k", [])
        assert a.key == "k"

    def test_asset_as_set_member(self):
        a = MemoryAsset("x")
        b = MemoryAsset("x")
        s = {a, b}
        assert len(s) == 1


# ---------------------------------------------------------------------------
# ExternalAsset (via SimpleExternalAsset)
# ---------------------------------------------------------------------------

class TestExternalAsset:
    def test_initial_state(self):
        ea = SimpleExternalAsset("ext", value=42)
        assert ea.status == AssetStatus.Unavailable
        assert ea._cached_val is AssetStatus.Unavailable
        assert ea._pending_val is None

    async def test_first_poll_returns_unavailable_timestamp(self):
        ea = SimpleExternalAsset("ext", value=42)
        ts = await ea.timestamp()
        assert ts is AssetStatus.Unavailable

    async def test_first_poll_stores_pending_value(self):
        ea = SimpleExternalAsset("ext", value=42)
        await ea.timestamp()
        assert ea._pending_val == 42

    async def test_build_commits_pending_value(self):
        ea = SimpleExternalAsset("ext", value=42)
        await ea.timestamp()
        await ea.build()
        assert ea._cached_val == 42
        ts = await ea.timestamp()
        assert isinstance(ts, plm.DateTime)

    async def test_changed_value_marks_stale(self):
        ea = SimpleExternalAsset("ext", value=1)
        await ea.timestamp()
        await ea.build()
        # Simulate status becoming Current
        ea.status = AssetStatus.Current
        # Now change the external value
        ea.set_value(2)
        await ea.timestamp()
        assert ea.status == AssetStatus.Stale

    async def test_unchanged_value_does_not_change_status(self):
        ea = SimpleExternalAsset("ext", value=1)
        await ea.timestamp()
        await ea.build()
        ea.status = AssetStatus.Current
        # same value – no diff
        await ea.timestamp()
        assert ea.status == AssetStatus.Current

    async def test_min_polling_interval_respected(self):
        ea = SimpleExternalAsset("ext", value=1)
        ea.min_polling_interval = plm.duration(hours=1)
        await ea.timestamp()
        first_poll = ea.last_poll
        # Change value; poll should NOT run again immediately
        ea.set_value(999)
        await ea.timestamp()
        assert ea._pending_val != 999  # still old pending value (or None from build)
        assert ea.last_poll == first_poll

    async def test_build_with_no_pending_value_is_noop(self):
        ea = SimpleExternalAsset("ext", value=1)
        # No timestamp() call, so _pending_val is still None
        await ea.build()
        assert ea._cached_val is AssetStatus.Unavailable

    def test_external_asset_concrete_subclass_can_be_instantiated(self):
        # ExternalAsset does not use ABC enforcement; concrete subclasses
        # simply override get() and diff().
        ea = SimpleExternalAsset("k", value=None)
        assert ea is not None
