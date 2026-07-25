"""Tests for fwirl.schedule – Schedule."""
import asyncio

import pendulum as plm
import pytest

from fwirl.schedule import Schedule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _noop(**kwargs):
    pass


async def _recording_coro(calls, **kwargs):
    calls.append(kwargs)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestScheduleInit:
    def test_regular_schedule(self):
        sch = Schedule("s1", _noop, {}, cron_string="0 * * * *")
        assert sch.name == "s1"
        assert sch.cron_string == "0 * * * *"
        assert sch.cron is not None
        assert not sch.paused

    def test_immediate_once_schedule(self):
        sch = Schedule("s2", _noop, {}, immediate_once=True)
        assert sch.cron_string == "IMMEDIATE"
        assert sch.cron is None

    def test_kwargs_stored(self):
        sch = Schedule("s3", _noop, {"x": 1, "y": 2})
        assert sch.kwargs == {"x": 1, "y": 2}

    def test_repr(self):
        sch = Schedule("s4", _noop, {}, cron_string="* * * * *")
        r = repr(sch)
        assert "s4" in r
        assert "* * * * *" in r
        assert "Paused=False" in r


# ---------------------------------------------------------------------------
# next()
# ---------------------------------------------------------------------------

class TestScheduleNext:
    def test_immediate_returns_sentinel(self):
        sch = Schedule("s", _noop, {}, immediate_once=True)
        assert sch.next() is Schedule.IMMEDIATE

    def test_cron_returns_positive_seconds(self):
        sch = Schedule("s", _noop, {}, cron_string="* * * * *")  # every minute
        secs = sch.next()
        assert isinstance(secs, (int, float))
        assert 0 <= secs <= 60

    def test_cron_with_reference_time(self):
        sch = Schedule("s", _noop, {}, cron_string="0 12 * * *")  # noon every day
        ref = plm.datetime(2020, 1, 1, 11, 0, 0, tz='UTC')
        secs = sch.next(dt=ref)
        assert 3599 <= secs <= 3601  # ~1 hour


# ---------------------------------------------------------------------------
# pause / unpause / is_paused
# ---------------------------------------------------------------------------

class TestSchedulePauseUnpause:
    def test_initially_not_paused(self):
        sch = Schedule("s", _noop, {})
        assert not sch.is_paused()

    def test_pause_sets_flag(self):
        sch = Schedule("s", _noop, {})
        sch.pause()
        assert sch.is_paused()

    def test_unpause_clears_flag(self):
        sch = Schedule("s", _noop, {})
        sch.pause()
        sch.unpause()
        assert not sch.is_paused()

    def test_double_pause_still_paused(self):
        sch = Schedule("s", _noop, {})
        sch.pause()
        sch.pause()
        assert sch.is_paused()


# ---------------------------------------------------------------------------
# generate_coroutine
# ---------------------------------------------------------------------------

class TestScheduleGenerateCoroutine:
    async def test_generates_awaitable(self):
        calls = []

        async def record(**kw):
            calls.append(kw)

        sch = Schedule("s", record, {"a": 1})
        coro = sch.generate_coroutine()
        await coro
        assert calls == [{"a": 1}]

    async def test_each_call_creates_fresh_coroutine(self):
        calls = []

        async def record(**kw):
            calls.append(True)

        sch = Schedule("s", record, {})
        await sch.generate_coroutine()
        await sch.generate_coroutine()
        assert len(calls) == 2

    async def test_kwargs_forwarded_correctly(self):
        received = {}

        async def capture(**kw):
            received.update(kw)

        sch = Schedule("s", capture, {"x": 42, "y": "hello"})
        await sch.generate_coroutine()
        assert received == {"x": 42, "y": "hello"}
