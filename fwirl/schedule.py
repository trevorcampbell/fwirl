from crontab import CronTab

class Schedule:
    def __init__(self, name, cron_string, num_runs, func, kwargs, modifies_graph):
        self.name = name
        self.cron = CronTab(cron_string) if cron_string else None
        self.cron_string = cron_string
        self.num_runs = num_runs
        self.func = func
        self.kwargs = kwargs
        self.modifies_graph = modifies_graph
        self.paused = False

    def next(self, dt=None):
        return self.cron.next(dt, default_utc=True) # return number of seconds until next event

    def pause(self):
        self.paused = True

    def unpause(self):
        self.paused = False

    def is_paused(self):
        return self.paused

    def generate_coroutine(self):
        return self.func(**self.kwargs)

    def __repr__(self):
        return self.__class__.__name__ + f"({self.name}, {self.cron_string if self.cron_string else 'NOW'}, Remaining Runs={self.num_runs}, Paused={self.paused})"
