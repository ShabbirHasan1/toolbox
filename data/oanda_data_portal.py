import os
import pytest
import pandas as pd
import numpy as np
from ..broker import Oanda
from zipline.data.data_portal import DataPortal

from sqlalchemy import (
    create_engine,
    select,
    Table,
    MetaData,
    Column,
    Integer,
    String
)

from zipline.data.minute_bars import MinuteBarReader


class OandaDataPortal(DataPortal):
    def __init__(self, minute_reader, asset_finder):
        self.asset_finder = asset_finder
        self.minute_reader = minute_reader
        self.minute_reader.load_data_cache([37])
        self._adjustment_reader = None

    def get_spot_value(self, asset, field, dt, data_frequency):
        if data_frequency == 'daily':
            raise "Unimplemented"
        elif data_frequency == 'minute':
            return self.minute_reader.get_value(asset.sid,
                                                dt,
                                                field)

    def get_history_window(self, assets, end_dt, bar_count, frequency, field,
                           ffill=True):
        pass


class OandaMinuteReader(MinuteBarReader):
    def __init__(self, ohlcv_path, trading_calendar):
        echo = os.environ.get("SQLITE_ECHO", False)
        self.engine = create_engine('sqlite:///{}'.format(ohlcv_path),
                                    echo=echo)
        self.trading_calendar = trading_calendar
        self.oanda = Oanda(None)

    def load_data_cache(self, sids):
        self._cache = {}
        for s in sids:
            query = select([table(s)])
            self._cache[s] = pd.read_sql(query,
                                         self.engine,
                                         index_col='datetime',
                                         parse_dates=['datetime'])

    def last_available_dt(self):
        return self._cache.values[0].index[-1]

    def first_trading_day(self):
        return self._cache.values[0].index.days[0]

    def get_last_traded_dt(self, asset, dt):
        return self._cache.values[0].index[-1]

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
        pytest.set_trace()
        pass


def table(sid):
    metadata = MetaData()
    return Table('minute_bars_{}'.format(sid), metadata,
                 Column('datetime', String(30), primary_key=True),
                 Column('open', Integer, nullable=False),
                 Column('high', Integer, nullable=False),
                 Column('low', Integer, nullable=False),
                 Column('close', Integer, nullable=False),
                 Column('volume', Integer, nullable=False))
