import pytz
import pandas as pd
from datetime import time
from zipline.utils.calendars import TradingCalendar


class ForexCalendar(TradingCalendar):

    NYT_5PM = time(9)

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

    def special_opens_adhoc(self):
        return [
            (self.NYT_5PM, self._sunday_dates())
        ]

    def special_closes_adhoc(self):
        return [
            (self.NYT_5PM, self._friday_dates())
        ]

    def _friday_dates(self):
        return pd.date_range(start=self.schedule.index[0],
                             end=self.schedule.idnex[-1],
                             freq='W-FRI')

    def _sunday_dates(self):
        return pd.date_range(start=self.schedule.index[0],
                             end=self.schedule.idnex[-1],
                             freq='W-SUN')
