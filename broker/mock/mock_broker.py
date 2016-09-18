from .blotter import BracketBlotter
from .execution import (
        BracketedLimitOrder,
        BracketedStopOrder,
        BracketedStopLimitOrder,
        BracketedMarketOrder
)
from zipline.finance.execution import (
        LimitOrder,
        MarketOrder,
        StopLimitOrder,
        StopOrder,
)
from zipline.utils.math_utils import (
        round_if_near_integer
)


class Client(BracketBlotter):
    def __init__(self, algo, account_id, asset_finder, frequency='minute'):
        self.algo = algo
        self.account_id = account_id
        BracketBlotter.__init__(self, frequency, asset_finder)

    def create_order(self, instrument, amount,
                     limit=None, stop=None, expiry=None,
                     stop_loss=None, take_profit=None):
        """
        Creates an order. The order type depends on which argument is supplied.
        Supported types are any combinations of limit, stop, take_profit,
        stoploss orders.

        Parameters
        ----------
        instrument: zipline Asset
            The equity to be ordered. Should respond to .symbol() and .sid()

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
        style = self.convert_order_params_for_blotter(limit,
                                                      stop,
                                                      stop_loss,
                                                      take_profit)
        return self.order(instrument, amount, style)

    def convert_order_params_for_blotter(self,
                                         limit_price,
                                         stop_price,
                                         stop_loss,
                                         take_profit):
        if stop_loss is None and take_profit is None:
            if limit_price and stop_price:
                return StopLimitOrder(limit_price, stop_price)
            if limit_price:
                return LimitOrder(limit_price)
            if stop_price:
                return StopOrder(stop_price)
            else:
                return MarketOrder()
        else:
            if limit_price and stop_price:
                return BracketedStopLimitOrder(stop_price, limit_price, stop_loss, take_profit)
            if limit_price:
                return BracketedLimitOrder(limit_price, stop_loss, take_profit)
            if stop_price:
                return BracketedStopOrder(stop_price, stop_loss, take_profit)
            else:
                return BracketedMarketOrder(stop_loss, take_profit)
