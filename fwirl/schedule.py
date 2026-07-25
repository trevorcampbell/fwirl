from crontab import CronTab
import pendulum as plm


class Schedule:
    """A named, optionally-recurring job attached to an :class:`~fwirl.AssetGraph`.

    :class:`Schedule` objects are created internally by
    :meth:`AssetGraph.schedule <fwirl.AssetGraph.schedule>` and are not
    normally constructed directly by user code.

    Args:
        name: A unique string identifier for this schedule.
        func: The async callable to invoke when the schedule fires.
        kwargs: Keyword arguments passed to *func* on each invocation.
        cron_string: A standard cron expression (``"* * * * *"`` by default)
            that controls when the schedule fires.  Ignored when
            *immediate_once* is ``True``.
        immediate_once: When ``True`` the schedule fires once as soon as
            possible and is then removed from the graph.
    """

    IMMEDIATE = plm.datetime(1970, 1, 1, 0, 0, 0)

    def __init__(self, name, func, kwargs, cron_string='* * * * *', immediate_once=False):
        self.name = name
        if immediate_once:
            self.cron_string = 'IMMEDIATE'
            self.cron = None
        else:
            self.cron_string = cron_string
            self.cron = CronTab(cron_string)
        self.func = func
        self.kwargs = kwargs
        self.paused = False

    def next(self, dt=None):
        """Return seconds until the next scheduled firing, or :attr:`IMMEDIATE`.

        Args:
            dt: Reference :class:`pendulum.DateTime`; defaults to now.

        Returns:
            Number of seconds (float) until the next occurrence, or
            :attr:`IMMEDIATE` for one-shot schedules.
        """
        if self.cron:
            return self.cron.next(dt, default_utc=True)  # return number of seconds until next event
        return Schedule.IMMEDIATE

    def pause(self):
        """Prevent this schedule from firing until :meth:`unpause` is called."""
        self.paused = True

    def unpause(self):
        """Allow this schedule to fire again after a :meth:`pause`."""
        self.paused = False

    def is_paused(self):
        """Return ``True`` if the schedule is currently paused.

        Returns:
            bool: Paused state.
        """
        return self.paused

    def generate_coroutine(self):
        """Create and return the coroutine for the next scheduled execution.

        Returns:
            A coroutine object ready to be awaited.
        """
        return self.func(**self.kwargs)

    def __repr__(self):
        return self.__class__.__name__ + f"({self.name}, {self.cron_string}, Paused={self.paused})"
