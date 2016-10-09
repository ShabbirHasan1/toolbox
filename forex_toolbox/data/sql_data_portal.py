import os
import pandas as pd
import numpy as np

from .. import utils

from zipline.utils.calendars import get_calendar
from zipline.data.data_portal import DataPortal
from zipline.data.history_loader import MinuteHistoryLoader

from sqlalchemy import (
    create_engine,
    select, and_,
    Table,
    MetaData,
    Column,
    Integer,
    String
)

from zipline.data.minute_bars import MinuteBarReader


OHLCVP_FIELDS = frozenset([
    "open", "high", "low", "close", "volume", "price"
])


class SqlDataPortal(DataPortal):
    def __init__(self, minute_reader, asset_finder):
        self.asset_finder = asset_finder
        self.minute_reader = minute_reader
        self.minute_reader.load_data_cache([37])

        self._adjustment_reader = None

        self.trading_calendar = minute_reader.trading_calendar
        self._minute_history_loader = MinuteHistoryLoader(self.trading_calendar,
                                                          minute_reader,
                                                          None)

        self._first_trading_minute = self.trading_calendar.schedule.iloc[0]['market_open'].tz_localize("UTC")

    def get_spot_value(self, asset, field, dt, data_frequency):
        if data_frequency == 'daily':
            raise "Unimplemented"
        elif data_frequency == 'minute':
            return self.minute_reader.get_value(asset.sid,
                                                dt,
                                                field)


class SqlMinuteReader(MinuteBarReader):
    def __init__(self, db_url, trading_calendar=None):
        echo = os.environ.get("SQL_ECHO", False) == 'true'
        self.engine = create_engine(db_url,
                                    echo=echo)
        self.trading_calendar = trading_calendar or get_calendar("NYSE")

    def load_data_cache(self, sids):
        self._cache = {}
        for s in sids:
            symbol = utils.symbol(s)
            s_table = table(symbol)
            query = select([s_table]) \
                        .where(
                            and_(
                                s_table.c.datetime >= self.trading_calendar.opens()[0], # Pending PR to remove method call
                                s_table.c.datetime <= self.trading_calendar.closes[-1]
                            )
                        )

            self._cache[s] = pd.read_sql(query,
                                         self.engine,
                                         index_col='datetime',
                                         parse_dates=['datetime'])

    @property
    def days_of_data(self):
        return [v for (_, v) in self._cache.items()][0].index

    @property
    def last_available_dt(self):
        s = self.days_of_data[-1].tz_localize("UTC") \
                .replace(hour=0, minute=0)
        (_, close) = self.trading_calendar.open_and_close_for_session(s)
        return close

    @property
    def first_trading_day(self):
        return self.days_of_data[0]

    @property
    def get_last_traded_dt(self, asset, dt):
        return self.days_of_data[-1]

    def get_value(self, sid, dt, field):
        if field == 'price':
            field = 'close'
        try:
            val = self._cache[sid].ix[dt, field]
        except KeyError:
            if field == 'volume':
                return 0
            else:
                return np.nan
        if field == 'volume':
            return int(val)
        else:
            return int(val) * utils.float_multiplier(sid)

    def load_raw_arrays(self, fields, start_dt, end_dt, sids):
        """
        Parameters
        ----------
        fields : list of str
           'open', 'high', 'low', 'close', or 'volume'
        start_dt: Timestamp
           Beginning of the window range.
        end_dt: Timestamp
           End of the window range.
        sids : list of int
           The asset identifiers in the window.

        Returns
        -------
        list of np.ndarray
            A list with an entry per field of ndarrays with shape
            (minutes in range, sids) with a dtype of float64, containing the
            values for the respective field over start and end dt range.
        """
        results = []
        # index = self._cache[sids[0]].index
        # interested_period = index[start_dt:end_dt]

        for field in fields:
            df = pd.DataFrame()
            '''
            # if field != 'volume':
                # df = pd.DataFrame(np.nan,
                                  # index=interested_period,
                                  # columns=sids)
            # else:
                # df = pd.DataFrame(0,
                                  # index=interested_period,
                                  # columns=sids)
                                  '''
            for s in sids:
                df[s] = self._cache[s][start_dt:end_dt][field].copy()
            if field != 'volume':
                df[s] = df[s] * utils.float_multiplier(s)
            results.append(df.as_matrix())

        return results


def table(symbol):
    metadata = MetaData()
    return Table('minute_bars_{}'.format(symbol), metadata,
                 Column('datetime', String(30), primary_key=True),
                 Column('open', Integer, nullable=False),
                 Column('high', Integer, nullable=False),
                 Column('low', Integer, nullable=False),
                 Column('close', Integer, nullable=False),
                 Column('volume', Integer, nullable=False))
