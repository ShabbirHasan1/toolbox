import os
import pytest
import pandas as pd
import numpy as np
from ..broker import Oanda
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


class OandaDataPortal(DataPortal):
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


class OandaMinuteReader(MinuteBarReader):
    def __init__(self, db_url, trading_calendar):
        echo = os.environ.get("SQL_ECHO", False) == 'true'
        self.engine = create_engine(db_url,
                                    echo=echo)
        self.trading_calendar = trading_calendar
        self.oanda = Oanda(None)

    def load_data_cache(self, sids):
        self._cache = {}
        for s in sids:
            symbol = self.oanda.symbol(s)
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
            return int(val) * self.oanda.float_multiplier(sid)

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
                df[s] = df[s] * self.oanda.float_multiplier(s)
            results.append(df.as_matrix())

        return results

        '''


        shape = num_minutes, len(sids)

        for field in fields:
            if field != 'volume':
                out = np.full(shape, np.nan)
            else:
                out = np.zeros(shape, dtype=np.uint32)



        start_idx = self._find_position_of_minute(start_dt)
        end_idx = self._find_position_of_minute(end_dt)

        num_minutes = (end_idx - start_idx + 1)

        results = []

        indices_to_exclude = self._exclusion_indices_for_range(
            start_idx, end_idx)
        if indices_to_exclude is not None:
            for excl_start, excl_stop in indices_to_exclude:
                length = excl_stop - excl_start + 1
                num_minutes -= length

        shape = num_minutes, len(sids)

        for field in fields:
            if field != 'volume':
                out = np.full(shape, np.nan)
            else:
                out = np.zeros(shape, dtype=np.uint32)

            for i, sid in enumerate(sids):
                carray = self._open_minute_file(field, sid)
                values = carray[start_idx:end_idx + 1]
                if indices_to_exclude is not None:
                    for excl_start, excl_stop in indices_to_exclude[::-1]:
                        excl_slice = np.s_[
                            excl_start - start_idx:excl_stop - start_idx + 1]
                        values = np.delete(values, excl_slice)

                where = values != 0
                # first slice down to len(where) because we might not have
                # written data for all the minutes requested
                if field != 'volume':
                    out[:len(where), i][where] = (
                        values[where] * self._ohlc_ratio_inverse_for_sid(sid))
                else:
                    out[:len(where), i][where] = values[where]

            results.append(out)
        return results
        '''


def table(symbol):
    metadata = MetaData()
    return Table('minute_bars_{}'.format(symbol), metadata,
                 Column('datetime', String(30), primary_key=True),
                 Column('open', Integer, nullable=False),
                 Column('high', Integer, nullable=False),
                 Column('low', Integer, nullable=False),
                 Column('close', Integer, nullable=False),
                 Column('volume', Integer, nullable=False))
