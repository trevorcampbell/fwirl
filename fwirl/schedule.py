from crontab import CronTab
import pendulum as plm


class Schedule:
    IMMEDIATE = plm.datetime(1970, 1, 1, 0, 0, 0)

    def __init__(self, name, func, kwargs, cron_string = '* * * * *', immediate_once = False):
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
        if self.cron:
            return self.cron.next(dt, default_utc=True) # return number of seconds until next event
        return Schedule.IMMEDIATE

    def pause(self):
        self.paused = True

    def unpause(self):
        self.paused = False

    def is_paused(self):
        return self.paused

    def generate_coroutine(self):
        return self.func(**self.kwargs)

    def __repr__(self):
        return self.__class__.__name__ + f"({self.name}, {self.cron_string}, Paused={self.paused})"
