import pytz
from datetime import time
from zipline.utils.calendars import TradingCalendar

class ForexCalendar(TradingCalendar):
    @property
    def name(self):
        return "forex"

    @property
    def tz(self):
        return pytz.UTC

    @property
    def open_time(self):
        return time(0, 0)

    @property
    def close_time(self):
        return time(23, 59)
