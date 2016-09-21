from zipline.finance.order import Order


class BracketOrder(Order):
    __slots__ = ["sl_order", "tp_order", "take_profit", "stop_loss",
                 "base_order_id", "txn_price", "trailling"] + Order.__slots__

    def __init__(self, dt, sid, amount, stop=None, limit=None, base_order_id=None,
                 take_profit=None, stop_loss=None, trailling=None, filled=0,
                 commission=0, id=None, tp_order_id=None, sl_order_id=None):
        Order.__init__(self, dt, sid, amount, stop, limit, filled,
                       commission, id)
        self.stop_loss = stop_loss
        self.take_profit = take_profit
        self.sl_order, self.tp_order = None, None
        self.trailling = trailling
        self.base_order_id = base_order_id
        self.txn_price = None

    def open_bracket(self, amount, price, dt):
        """
        Open orders in the oposite direction, limit and stoping at the
        given take_profit and stop_loss prices

        Parameters
        ----------
        amount: int
            The amount of base order being bracketted.
            amount should be > 0 the new position is long, and vice versa

        dt: datetime.datetime
            Datetime the bracket is placed. Should be the time when the base
            order has just been filled.

        Returns
        -------
        None
        """
        self.txn_price = price

        if amount and self.take_profit:
            self.tp_order = BracketOrder(dt, self.sid,
                                         - amount,
                                         limit=self.take_profit,
                                         id=self.id + '_tp',
                                         base_order_id=self.id)

        if amount and self.stop_loss:
            self.sl_order = BracketOrder(dt, self.sid,
                                         - amount,
                                         stop=self.stop_loss,
                                         id=self.id + '_sl',
                                         base_order_id=self.id)
        return self.tp_order, self.sl_order

    def partially_cancel(self, amount):
        self.amount -= amount
