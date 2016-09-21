from zipline.finance.execution import (
    LimitOrder,
    StopOrder,
    StopLimitOrder,
    MarketOrder
)


class Bracketed(object):
    """
    Mixin for the bracketted order behaviour
    """
    def __init__(self, stop_loss, take_profit, trailling):
        self.sl = stop_loss
        self.tp = take_profit
        self.trailling = trailling

    def get_tp(self, _is_buy):
        return self.tp

    def get_sl(self, _is_buy):
        return self.sl

    def get_trailling(self, _is_buy):
        return self.trailling


class BracketedLimitOrder(Bracketed, LimitOrder):
    """
    Class encapsulating an order to be placed at the limit(upper bound) price,
    with take profit and stop loss levels
    """

    def __init__(self, limit_price=None, stop_price=None, trailling=None,
                 stop_loss=None, take_profit=None, exchange=None):
        LimitOrder.__init__(self, limit_price, exchange)
        Bracketed.__init__(self, stop_loss, take_profit, trailling)


class BracketedStopOrder(Bracketed, StopOrder):
    """
    Class encapsulating an order to be placed at the stop(lower bound)
    price, with take profit and stop loss levels
    """

    def __init__(self, limit_price=None, stop_price=None, trailling=None,
                 stop_loss=None, take_profit=None, exchange=None):
        StopOrder.__init__(self, stop_price, exchange)
        Bracketed.__init__(self, stop_loss, take_profit, trailling)


class BracketedStopLimitOrder(Bracketed, StopLimitOrder):
    """
    Class encapsulating an order to be placed between the limt(upper bound)
    and stop(lower bound) price, with take profit and stop loss levels
    """

    def __init__(self, limit_price=None, stop_price=None, trailling=None,
                 stop_loss=None, take_profit=None, exchange=None):
        StopLimitOrder.__init__(self, limit_price, stop_price, exchange)
        Bracketed.__init__(self, stop_loss, take_profit, trailling)


class BracketedMarketOrder(Bracketed, MarketOrder):
    """
    Class encapsulating an order to be placed at the current market price,
    with take profit and stop loss levels
    """

    def __init__(self, limit_price=None, stop_price=None, trailling=None,
                 stop_loss=None, take_profit=None, exchange=None):
        MarketOrder.__init__(self, exchange)
        Bracketed.__init__(self, stop_loss, take_profit, trailling)
