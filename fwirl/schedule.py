from crontab import CronTab

class Schedule:
    def __init__(self, name, cron_string, func, kwargs, modifies_graph):
        self.name = name
        self.cron = CronTab(cron_string)
        self.cron_string = cron_string
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
        return self.__class__.__name__ + f"({self.name}, '{self.cron_string}', Paused={self.paused})"
