import json
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
        self.load_instruments_info()
        if self.algo is not None:
            self.blotter = BracketBlotter(algo.blotter.data_frequency,
                                          algo.blotter.asset_finder)

    @property
    def sql_reader(self):
        if not hasattr(self, "_reader"):
            self._reader = SqlMinuteReader(os.environ.get("DATABASE_URL"))
        return self._reader

    def get_history(self,
                    instrument,
                    end_dt=None,
                    count=500,
                    resolution='m1',
                    candleFormat='midpoint'):
        """
        Returns
        -------
           candles : pd.DataFrame
               Indexed by datetime and has columns: ['openMid', 'highMid', 'lowMid', 'closeMid']
        """
        sid = self.sid(instrument)
        if not hasattr(self.sql_reader, '_cache') or sid not in self.sql_reader._cache:
            self.sql_reader.load_data_cache([sid])
        end_index = self.sql_reader._cache[self.sid(instrument)].index.get_loc(end_dt)
        return self.sql_reader._cache[self.sid(instrument)][end_index-count:end_index]

    def create_order(self, instrument, amount,
                     limit=None, stop=None, expiry=None,
                     stop_loss=None, take_profit=None, trailling=None):
        """
        Creates an order. The order type depends on which argument is supplied.
        Supported types are any combinations of limit, stop, take_profit,
        stoploss orders.

        Parameters
        ----------
        instrument: zipline Asset
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
        if not self.algo._can_order_asset(instrument):
            return None

        # Truncate to the integer share count that's either within .0001 of
        # amount or closer to zero.
        # E.g. 3.9999 -> 4.0; 5.5 -> 5.0; -5.5 -> -5.0
        amount = int(round_if_near_integer(amount))

        # Raises a ZiplineError if invalid parameters are detected.
        # Also run the params through any registered trading_controls
        self.algo.validate_order_params(instrument,
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
        return self.blotter.order(instrument, amount, style)

    def sid(self, symbol):
        """
        Returns the arbitrary id assigned to the instrument
        symbol.

        See broker/oanda_instruments.json

        Return
        ------
        sid : int
        """
        return self.sym_sid_map[symbol]

    def symbol(self, sid):
        return self.sid_sym_map[sid]

    def display_name(self, sid):
        return self.sid_name_map[sid]

    def multiplier(self, instrument):
        return self.ohlc_ratio[instrument]

    def float_multiplier(self, sid):
        return self.inverse_ohlc_ratio[sid]

    def load_instruments_info(self):
        dir_path = os.path.dirname(os.path.realpath(__file__))
        with open('{}/oanda_instruments.json'.format(dir_path)) as data_file:
            self.instruments  = json.load(data_file)
            self.sid_sym_map  = {i['sid']: i['instrument'] for i in self.instruments}
            self.sym_sid_map  = {i['instrument']: i['sid'] for i in self.instruments}
            self.sid_name_map = {i['sid']: i['displayName'] for i in self.instruments}
            self.ohlc_ratio   = {i['instrument']: int(100 * 1 / float(i['pip'])) for i in self.instruments}
            self.inverse_ohlc_ratio = {i['sid']: float(i['pip'])/100.0 for i in self.instruments}


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
