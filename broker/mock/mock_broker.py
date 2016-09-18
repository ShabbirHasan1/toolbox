from .blotter import BracketBlotter
from zipline.finance.execution import (
        LimitOrder,
        MarketOrder,
        StopLimitOrder,
        StopOrder,
)


class Client(BracketBlotter):
    def __init__(self, account_id, asset_finder, frequency='minute'):
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
        self.order(instrument, amount, style=LimitOrder(1))
