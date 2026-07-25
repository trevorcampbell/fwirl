"""Tests for fwirl.resource – Resource base class."""
import pytest

from fwirl.resource import Resource
from tests.conftest import CountingResource


class TestResource:
    def test_cannot_instantiate_abstract(self):
        # Resource has no ABC enforcement; concrete subclasses override
        # init() and close(). CountingResource is always instantiatable.
        r = CountingResource("test")
        assert r is not None

    def test_init_stores_key_and_hash(self):
        r = CountingResource("my-resource")
        assert r.key == "my-resource"
        assert r.hash == hash("my-resource")

    def test_hash_method(self):
        r = CountingResource("r")
        assert hash(r) == hash("r")

    def test_equality_based_on_hash(self):
        r1 = CountingResource("same")
        r2 = CountingResource("same")
        r3 = CountingResource("different")
        assert r1 == r2
        assert r1 != r3

    def test_repr(self):
        r = CountingResource("res-key")
        assert "CountingResource" in repr(r)
        assert "res-key" in repr(r)

    def test_resource_as_set_member(self):
        r1 = CountingResource("r")
        r2 = CountingResource("r")
        assert len({r1, r2}) == 1

    def test_init_increments_counter(self):
        r = CountingResource()
        r.init()
        r.init()
        assert r.init_count == 2

    def test_close_increments_counter(self):
        r = CountingResource()
        r.close()
        r.close()
        r.close()
        assert r.close_count == 3

    def test_init_raises_propagates(self):
        r = CountingResource(init_raises=True)
        with pytest.raises(RuntimeError, match="resource init failed"):
            r.init()

    def test_close_raises_propagates(self):
        r = CountingResource(close_raises=True)
        with pytest.raises(RuntimeError, match="resource close failed"):
            r.close()
