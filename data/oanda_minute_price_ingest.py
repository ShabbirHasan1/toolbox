import os
import pandas as pd
from sqlalchemy import (
    create_engine,
    Table,
    MetaData,
    Column,
    Integer,
    String
)
from ..broker import Oanda


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

    def __init__(self, output_dir):
        echo = os.environ.get("SQLITE_ECHO", False)
        self.engine = create_engine('sqlite:///{}'.format(output_dir), echo=echo)
        self.broker = Oanda(os.environ.get("OANADA_ACCOUNT_ID", "test"))

    def url(self):
        return '%s/%s' % (self.broker.oanda.api_url, 'v1/candles')

    def run(self, symbol):
        """
        Request for 1 minute candles from oanda, and write to sqlite3.
        Prices are saved as integer, from multiplying with the actual
        price with the instrument's ohlc ratio, with the aim to support
        2dp of the instrument's pip size.

        For e.g., EUR_JPY pip size is 0.01, we'll store prices of precision
        1.0000, as integer 10000

        """
        candles = self.broker.get_history(symbol)
        df = pd.DataFrame(candles)
        df = df[df['complete']]
        df.set_index(pd.DatetimeIndex(df.time), inplace=True)
        df.rename(columns={'openMid': 'open',
                           'highMid': 'high',
                           'lowMid': 'low',
                           'closeMid': 'close'},
                  inplace=True)
        df = df[['open', 'high', 'low', 'close', 'volume']]

        convert_price_to_int(df, self.broker.ohlc_ratio[symbol])
        self.write_sid(self.broker.sid(symbol), df)

    def write_sid(self, sid, df):
        self.ensure_table(sid)
        self.delete_duplicates(sid, df)
        df.to_sql(name="minute_bars_{}".format(sid),
                  con=self.engine,
                  index_label="datetime",
                  if_exists='append')

    def ensure_table(self, sid):
        table(sid).create(self.engine, checkfirst=True)

    def delete_duplicates(self, sid, df):
        c = self.engine.connect()
        datetime_list = df.index.strftime("%Y-%m-%d %H:%M:%S.%f")
        t = table(sid)
        c.execute(t.delete().where(t.c.datetime.in_(datetime_list)))

    def on_success(self, data):
        """
        Aggregates tick data into a minute bar,
        then write with minute_bar_writer.

        Parameters
        ----------
        - data : response dict, loaded from stream response json
        """
        """
        tick = data['tick']
        tick_series = pending_bars[tick['instrument']]
        time = tick['time']

        if new_minute:
            bar = tick_series.resample('1Min').ohlc()
            bar['volumne'] = tick_series.resmaple('1Min').count()

            self.minute_bar_writer.write_sid(sid, bar[-1])

            pending_bar[tick['instrument']] = pd.Series([mid], index=[time])

        else:
            mid = (tick['bid'] + tick['ask']) / 2
            pending_bar[tick['instrument']].set_value(time, mid)
        """
        for s in self.on_success_listeners:
            s(self, data)

        self.minute_bar_writer.write()
        self.asset_db_writer.write()

    def on_error(self, data):
        """
        Backoff and try again
        """
        pass


def convert_price_to_int(df, ratio):
    df.open = (df.open * ratio).astype(int)
    df.high = (df.high * ratio).astype(int)
    df.low = (df.low * ratio).astype(int)
    df.close = (df.close * ratio).astype(int)


def table(sid):
    metadata = MetaData()
    return Table('minute_bars_{}'.format(sid), metadata,
                 Column('datetime', String(30), primary_key=True),
                 Column('open', Integer, nullable=False),
                 Column('high', Integer, nullable=False),
                 Column('low', Integer, nullable=False),
                 Column('close', Integer, nullable=False),
                 Column('volume', Integer, nullable=False))
