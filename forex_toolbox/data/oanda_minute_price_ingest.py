from ..zipline_extension.assets.asset_writer import AssetDBWriter
from ..zipline_extension.assets import AssetFinder, Equity

import os
import numpy as np
import pandas as pd
from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    Column,
    Integer,
    String,
    DateTime,
    select,
    exc
)
from ..broker import Oanda
from .. import utils
from zipline.assets.asset_writer import write_version_info
from zipline.assets.asset_db_schema import version_info, metadata, ASSET_DB_VERSION


class OandaMinutePriceIngest():
    """
    A long running process that uses oanda's HTTP Streaming API (see http://developer.oanda.com/rest-live/streaming/)
    to aggregate bid/ask ticks into minute OHLCV bar, and saved with minute_bar_writer.


    For live streaming, set the following env vars:
        - OANDA_ENV = practice
        - OANDA_ACCOUNT_IDJ = streaming_account_id
        - OANDA_ACCESS_TOKEN = xxx

    Live/backtest algos can access the persisted price history.

    """

    VERSION = 0

    def __init__(self, db_url):
        self.broker = Oanda(os.environ.get("OANADA_ACCOUNT_ID", "test"))

        echo = os.environ.get("SQL_ECHO", False) == 'true'
        self.engine = create_engine(db_url,
                                    echo=echo)

    def run(self, symbol, end=None):
        """
        Request for 1 minute candles from oanda, and write to sqlite3.
        Prices are saved as integer, from multiplying with the actual
        price with the instrument's ohlc ratio, with the aim to support
        2dp of the instrument's pip size.

        For e.g., EUR_JPY pip size is 0.01, we'll store prices of precision
        1.0000, as integer 10000

        """
        if end:
            end = end.isoformat()

        candles = self.broker.get_history(symbol, end=end)
        df = pd.DataFrame(candles)
        df = df[df['complete']]

        # can't write with UTC, so we keep the index naiive.
        # See https://github.com/pydata/pandas/issues/9086
        df.set_index(pd.DatetimeIndex(df.time), inplace=True)

        df.rename(columns={'openMid': 'open',
                           'highMid': 'high',
                           'lowMid': 'low',
                           'closeMid': 'close'},
                  inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume']]

        convert_price_to_int(df, utils.multiplier(symbol))

        self._write_symbol(symbol, df)
        self._write_asset_info(symbol, df)

    def url(self):
        return '%s/%s' % (self.broker.oanda.api_url, 'v1/candles')

    def _write_symbol(self, symbol, df):
        self._ensure_table(symbol)
        self._delete_duplicate_minutes(symbol, df)
        df.to_sql(name="minute_bars_{}".format(symbol),
                  con=self.engine,
                  index_label="datetime",
                  dtype={"datetime": String(30)},
                  if_exists='append')

    def _write_asset_info(self, symbol, df):
        """
          - Loads existing asset info,
          - compares and update the date boundaries,
          - then delete the asset info row, because AssetDBWriter doesn't
                support replacing.
          - And finally write the new asset info
        """

        try:
            reader = AssetFinder(self.engine)
        except exc.InvalidRequestError as err:
            if 'Could not reflect' in str(err):
                metadata.create_all(self.engine, checkfirst=True)
                self._ensure_version()
            reader = AssetFinder(self.engine)

        sid = utils.sid(symbol)
        asset = reader.retrieve_asset(sid, default_none=True) \
                or Equity(sid, "forex",
                          symbol=symbol,
                          asset_name=utils.display_name(sid))
        self.asset_metadata = build_tzaware_metadata(asset)

        # Construct a tz-aware index, for date comparison
        index = df.index.tz_localize('UTC')

        changed = False
        if self.asset_metadata.ix[sid, 'start_date'] is pd.NaT or self.asset_metadata.start_date.ix[sid] > index[0]:
            changed = True
            self.asset_metadata.ix[sid, 'start_date'] = index[0]

        if self.asset_metadata.ix[sid, 'end_date'] is pd.NaT or self.asset_metadata.end_date.ix[sid] < index[-1]:
            changed = True
            self.asset_metadata.ix[sid, 'end_date'] = index[-1]
            self.asset_metadata.ix[sid, 'auto_close_date'] = index[-1] + pd.Timedelta(days=1)

        if changed:
            writer = AssetDBWriter(self.engine)
            self._delete_existing_asset_metadata(sid)
            self.asset_metadata['start_date'] = self.asset_metadata.start_date.dt.date
            self.asset_metadata['end_date'] = self.asset_metadata.end_date.dt.date
            self.asset_metadata['auto_close_date'] = self.asset_metadata.auto_close_date.dt.date
            writer.write(equities=self.asset_metadata.dropna())

    def _ensure_table(self, symbol):
        table(symbol).create(self.engine, checkfirst=True)

    def _delete_duplicate_minutes(self, symbol, df):
        c = self.engine.connect()
        datetime_list = df.index.strftime("%Y-%m-%d %H:%M:%S")
        t = table(symbol)
        c.execute(t.delete().where(t.c.datetime.in_(datetime_list)))
        c.close()

    def _delete_existing_asset_metadata(self, sid):
        meta = MetaData(self.engine, reflect=True)
        c = self.engine.connect()

        for t in ['equity_symbol_mappings', 'asset_router', 'equities']:
            table = meta.tables[t]
            c.execute(table.delete().where(table.c.sid == sid))

        c.close()

    def _ensure_version(self):
        meta = MetaData(self.engine, reflect=True)
        if 'version_info' not in meta.tables:
            write_version_info(self.engine.connect(),
                               version_info,
                               ASSET_DB_VERSION)
        else:
            version_table = meta.tables['version_info']
            version = self.engine.execute(select((version_table.c.version,))).scalar()
            if not version:
                write_version_info(self.engine.connect(),
                                   version_info,
                                   ASSET_DB_VERSION)


def convert_price_to_int(df, ratio):
    df.open = (df.open * ratio).astype(int)
    df.high = (df.high * ratio).astype(int)
    df.low = (df.low * ratio).astype(int)
    df.close = (df.close * ratio).astype(int)


def table(symbol):
    metadata = MetaData()
    return Table('minute_bars_{}'.format(symbol), metadata,
                 Column('datetime', DateTime, primary_key=True),
                 Column('open', Integer, nullable=False),
                 Column('high', Integer, nullable=False),
                 Column('low', Integer, nullable=False),
                 Column('close', Integer, nullable=False),
                 Column('volume', Integer, nullable=False))


def build_tzaware_metadata(asset):
    assert asset is not None
    df = pd.DataFrame(np.empty(1, dtype=[
        ('start_date',      'datetime64[ns]'),
        ('end_date',        'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('exchange',        'object'),
        ('symbol',          'object'),
        ('asset_name',      'object'),
        ]), index=[asset.sid])
    df['start_date'] = df.start_date.dt.tz_localize('UTC')
    df['end_date'] = df.end_date.dt.tz_localize('UTC')
    df['auto_close_date'] = df.auto_close_date.dt.tz_localize('UTC')

    df.ix[asset.sid] = asset.start_date, asset.end_date, asset.auto_close_date, asset.exchange, asset.symbol, asset.asset_name
    return df
