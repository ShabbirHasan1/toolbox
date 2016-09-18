from zipline.finance.execution import ExecutionStyle


class BracketedLimitOrder(ExecutionStyle):
    """
    Class encapsulating an order to be placed at the limit(upper bound) price,
    with take profit and stop loss levels
    """

    def __init__(self, limit_price, stop_loss, take_profit, exchange=None):
        self._exchange = exchange

    def get_limit_price(self, _is_buy):
        return None

    def get_stop_price(self, _is_buy):
        return None


class BracketedStopOrder(ExecutionStyle):
    """
    Class encapsulating an order to be placed at the stop(lower bound)
    price, with take profit and stop loss levels
    """

    def __init__(self, stop_price, stop_loss, take_profit, exchange=None):
        self._exchange = exchange

    def get_limit_price(self, _is_buy):
        return None

    def get_stop_price(self, _is_buy):
        return None


class BracketedStopLimitOrder(ExecutionStyle):
    """
    Class encapsulating an order to be placed between the limt(upper bound)
    and stop(lower bound) price, with take profit and stop loss levels
    """

    def __init__(self, stop_price, limit_price, stop_loss, take_profit,
                 exchange=None):
        self._exchange = exchange

    def get_limit_price(self, _is_buy):
        return None

    def get_stop_price(self, _is_buy):
        return None


class BracketedMarketOrder(ExecutionStyle):
    """
    Class encapsulating an order to be placed at the current market price,
    with take profit and stop loss levels
    """

    def __init__(self, stop_loss, take_profit, exchange=None):
        self._exchange = exchange

    def get_limit_price(self, _is_buy):
        return None

    def get_stop_price(self, _is_buy):
        return None
