from .. import BracketBlotter
from ..zipline_extension.execution import (
        BracketedLimitOrder,
        BracketedStopOrder,
        BracketedStopLimitOrder,
        BracketedMarketOrder
)
from zipline.utils.math_utils import (
        round_if_near_integer
)


class SimuBroker(object):
    def __init__(self, algo):
        self.algo = algo
        self.blotter = BracketBlotter(algo.blotter.data_frequency,
                                      algo.blotter.asset_finder)

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
        style = convert_order_params_for_blotter(limit,
                                                 stop,
                                                 stop_loss,
                                                 take_profit,
                                                 trailling)
        return self.blotter.order(instrument, amount, style)


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
