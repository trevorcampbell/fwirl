from crontab import CronTab

class Schedule:
    def __init__(self, cron_string, asset):
        self.cron = CronTab(cron_string)
        self.asset = asset
        self.paused = False

    def next(self, dt=None):
        return self.cron.next(dt) # return number of seconds until next event

    def pause(self):
        self.paused = True

    def unpause(self):
        self.paused = False

    def is_paused(self):
        return self.paused
