from .. import utils
import os
from datetime import timedelta
from ..zipline_extension import BracketBlotter
from ..zipline_extension.execution import (
        BracketedLimitOrder,
        BracketedStopOrder,
        BracketedStopLimitOrder,
        BracketedMarketOrder
)
from zipline.utils.math_utils import (
        round_if_near_integer
)

from ..data.sql_data_portal import SqlMinuteReader


class SimuBroker(object):
    def __init__(self, algo):
        self.algo = algo
        if self.algo is not None:
            self._reader = self.algo.data_portal.minute_reader
            self.blotter = BracketBlotter(algo.blotter.data_frequency,
                                          algo.blotter.asset_finder)

    @property
    def sql_reader(self):
        if not hasattr(self, "_reader"):
            db_url = os.environ.get('DATABASE_URL',
                                    'postgres://postgres:password@localhost:5435/test')
            self._reader = SqlMinuteReader(db_url)
        return self._reader

    def get_history(self,
                    instr,
                    end_dt=None,
                    start_dt=None,
                    count=None,
                    resolution='M1',
                    candleFormat='midpoint'):
        """
        Params
        ------
        - instr : zipline Asset. Responds to '.sid'

        - end_dt : pandas.Timestamp

        - start_dt : pandas.Timestamp
            Optioanl, either give start_dt or count

        - count : int
            Optioanl, either give start_dt or count

        - resolution : string
            Oanda's resolution flags, "M1", "M5", "H2", "H6", etc

        - candleFormat: string
            Simu broker only supports 'midpoint' for now. Live broker this can
            be 'bidask'
        Returns
        -------
           candles : pd.DataFrame
               Indexed by datetime and has columns: ['openMid', 'highMid', 'lowMid', 'closeMid']
        """
        sid = instr.sid
        if not hasattr(self.sql_reader, '_cache') or sid not in self.sql_reader._cache:
            self.sql_reader.load_data_cache([sid])

        if end_dt not in self.sql_reader._cache[instr.sid].index:
            return None

        end_index = self.sql_reader._cache[instr.sid].index.get_loc(end_dt)
        if count:
            start_index = end_index - count * (time_delta(resolution)/timedelta(minutes=1))
            start_index = int(start_index)
        elif start_dt:
            cache = self.sql_reader._cache[instr.sid]
            while start_dt not in cache.index:
                start_dt = start_dt - pd.DateOffset(minutes=1)
            start_index = cache.index.get_loc(start_dt)

        df = self.sql_reader._cache[instr.sid][start_index:end_index]
        df = df.resample(oanda_to_pandas(resolution)).agg({'open': 'first',
                                                           'high': 'max',
                                                           'low': 'min',
                                                           'close': 'last',
                                                           'volume': 'sum'})
        df['volume'] = df['volume'] * 100000000
        multiplier = utils.float_multiplier(instr.sid)
        for field in ['open', 'high', 'low', 'close']:
            df[field] = df[field] * multiplier
        df.rename(columns={'open': 'openMid',
                           'high': 'highMid',
                           'low': 'lowMid',
                           'close': 'closeMid'},
                  inplace=True)

        return df

    def create_order(self, instr, amount,
                     limit=None, stop=None, expiry=None,
                     stop_loss=None, take_profit=None, trailling=None):
        """
        Creates an order. The order type depends on which argument is supplied.
        Supported types are any combinations of limit, stop, take_profit,
        stoploss orders.

        Parameters
        ----------
        instr: zipline Asset
            The equity to be ordered.
            Can be obtained from zipline_api.symbol("XX")

        amount: int
            The amount of shares to order. If ``amount`` is positive, this is
            the number of shares to buy or cover. If ``amount`` is negative,
            this is the number of shares to sell or short.

        limit: float, optional
            Limit price for the order

        stop: float, optional
            Stop price for the order

        stop_loss: float, optional
            Stop loss price for the order

        take_profit: float, optional
            Take profit price for the order

        Returns
        -------
        order_id: str
        A unique id for this order
        """
        if not self.algo._can_order_asset(instr):
            return None

        # Truncate to the integer share count that's either within .0001 of
        # amount or closer to zero.
        # E.g. 3.9999 -> 4.0; 5.5 -> 5.0; -5.5 -> -5.0
        amount = int(round_if_near_integer(amount))

        # Raises a ZiplineError if invalid parameters are detected.
        # Also run the params through any registered trading_controls
        self.algo.validate_order_params(instr,
                                        amount,
                                        limit,
                                        stop, style=None)

        # Convert deprecated limit_price and stop_price parameters to use
        # ExecutionStyle objects.
        style = convert_order_params_for_blotter(limit,
                                                 stop,
                                                 stop_loss,
                                                 take_profit,
                                                 trailling)
        return self.blotter.order(instr, amount, style)


def convert_order_params_for_blotter(limit_price,
                                     stop_price,
                                     stop_loss,
                                     take_profit,
                                     trailling):
    if limit_price and stop_price:
        return BracketedStopLimitOrder(stop_price=stop_price,
                                       limit_price=limit_price,
                                       stop_loss=stop_loss,
                                       take_profit=take_profit,
                                       trailling=trailling)
    if limit_price:
        return BracketedLimitOrder(stop_price=stop_price,
                                   limit_price=limit_price,
                                   stop_loss=stop_loss,
                                   take_profit=take_profit,
                                   trailling=trailling)
    if stop_price:
        return BracketedStopOrder(stop_price=stop_price,
                                  limit_price=limit_price,
                                  stop_loss=stop_loss,
                                  take_profit=take_profit,
                                  trailling=trailling)
    else:
        return BracketedMarketOrder(stop_price=stop_price,
                                    limit_price=limit_price,
                                    stop_loss=stop_loss,
                                    take_profit=take_profit,
                                    trailling=trailling)


def time_delta(resolution):
    """
    Params
    ------
    resolution : string
        "M1", "M15", etc

    Returns
    -------
    datetime.timedelta
    """
    h = {}
    value = resolution[1:]
    if value == '':
        value = 1
    else:
        value = int(value)
    h[resolution[0]] = value
    return timedelta(minutes=h.get('M', 0),
                     seconds=h.get('S', 0),
                     hours=h.get('H', 0))


def oanda_to_pandas(resolution):

    """
    Params
    ------
    resolution : string
        "M1", "M15", etc

    Returns
    -------
    string
        pandas compatible resample frequency: "1Min", "1H", "5S"
    """
    f = "{}{}".format(resolution[1:], resolution[0])
    if f != "M":
        f = f.replace('M', 'Min')
    return f
