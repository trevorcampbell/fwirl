from crontab import CronTab

class Schedule:
    def __init__(self, action, cron_string, asset):
        self.action = action
        self.cron = CronTab(cron_string)
        self.cron_string = cron_string
        self.asset = asset
        self.paused = False

    def next(self, dt=None):
        return self.cron.next(dt, default_utc=True) # return number of seconds until next event

    def pause(self):
        self.paused = True

    def unpause(self):
        self.paused = False

    def is_paused(self):
        return self.paused

    def __repr__(self):
        return self.__class__.__name__ + f"({self.action}, '{self.cron_string}', Paused={self.paused}, Asset={self.asset})"
