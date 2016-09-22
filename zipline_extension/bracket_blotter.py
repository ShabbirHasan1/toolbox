#
# Copyright 2015 Quantopian, Inc.
#
# Modifications Copyright 2016 Bernoullio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from logbook import Logger

from zipline.finance.blotter import Blotter

from .bracket_order import BracketOrder
from collections import defaultdict

log = Logger('Blotter')
warning_logger = Logger('AlgoWarning')


class BracketBlotter(Blotter):
    def __init__(self, data_frequency, asset_finder, slippage_func=None,
                 commission=None, cancel_policy=None):
        Blotter.__init__(self, data_frequency, asset_finder, slippage_func,
                         commission, cancel_policy)
        self.profit_orders = defaultdict(list)
        self.loss_orders = defaultdict(list)

    def order(self, sid, amount, style, order_id=None):

        # something could be done with amount to further divide
        # between buy by share count OR buy shares up to a dollar amount
        # numeric == share count  AND  "$dollar.cents" == cost amount

        """
        amount > 0 :: Buy/Cover
        amount < 0 :: Sell/Short
        Market order:    order(sid, amount)
        Limit order:     order(sid, amount, style=LimitOrder(limit_price))
        Stop order:      order(sid, amount, style=StopOrder(stop_price))
        StopLimit order: order(sid, amount, style=StopLimitOrder(limit_price,
                               stop_price))
        """
        if amount == 0:
            # Don't bother placing orders for 0 shares.
            return
        elif amount > self.max_shares:
            # Arbitrary limit of 100 billion (US) shares will never be
            # exceeded except by a buggy algorithm.
            raise OverflowError("Can't order more than %d shares" %
                                self.max_shares)

        is_buy = (amount > 0)
        order = BracketOrder(
            dt=self.current_dt,
            sid=sid,
            amount=amount,
            stop=style.get_stop_price(is_buy),
            limit=style.get_limit_price(is_buy),
            take_profit=None or style.get_tp(is_buy),
            stop_loss=None or style.get_sl(is_buy),
            trailling=None or style.get_trailling(is_buy),
            id=order_id
        )

        self.open_orders[order.sid].append(order)
        self.orders[order.id] = order
        self.new_orders.append(order)

        return order.id

    def cancel(self, order_id, relay_status=True):
        if order_id not in self.orders:
            return

        cur_order = self.orders[order_id]

        if cur_order.open:
            order_list = self.open_orders[cur_order.sid]
            if cur_order in order_list:
                order_list.remove(cur_order)

            if cur_order in self.new_orders:
                self.new_orders.remove(cur_order)
            cur_order.cancel()
            cur_order.dt = self.current_dt

            if relay_status:
                # we want this order's new status to be relayed out
                # along with newly placed orders.
                self.new_orders.append(cur_order)

    def get_transactions(self, bar_data):
        """
        Creates a list of transactions based on the current open orders,
        slippage model, and commission model.

        Parameters
        ----------
        bar_data: zipline._protocol.BarData

        Notes
        -----
        This method book-keeps the blotter's open_orders dictionary, so that
         it is accurate by the time we're done processing open orders.

        Returns
        -------
        transactions_list: List
            transactions_list: list of transactions resulting from the current
            open orders.  If there were no open orders, an empty list is
            returned.

        commissions_list: List
            commissions_list: list of commissions resulting from filling the
            open orders.  A commission is an object with "sid" and "cost"
            parameters.

        closed_orders: List
            closed_orders: list of all the orders that have filled.
        """

        closed_orders = []
        transactions = []
        commissions = []

        if self.open_orders:
            assets = self.asset_finder.retrieve_all(self.open_orders)
            asset_dict = {asset.sid: asset for asset in assets}

            for sid, asset_orders in self.open_orders.items():
                asset = asset_dict[sid]

                for order, txn in \
                        self.slippage_func(bar_data, asset, asset_orders):
                    additional_commission = \
                        self.commission.calculate(order, txn)

                    if additional_commission > 0:
                        commissions.append({
                            "sid": order.sid,
                            "order": order,
                            "cost": additional_commission
                        })

                    transactions.append(txn)
                    order.filled += txn.amount
                    order.commission += additional_commission

                    if not order.open:
                        closed_orders.append(order)
                        if order.base_order_id:
                            if order.limit:
                                self.profit_orders[order.sid].append(order)
                                self.cancel(order.other_bracket_id,
                                            relay_status=False)
                            elif order.stop:
                                self.loss_orders[order.sid].append(order)
                                self.cancel(order.other_bracket_id,
                                            relay_status=False)

                    # for base_order only:
                    if order.base_order_id is None:
                        remaining_amount = self.close_existing_brackets(
                            asset=order.sid,
                            amount=order.filled)

                        new_orders = order.open_bracket(remaining_amount,
                                                        txn.price, txn.dt)
                        new_orders = list(filter(None, new_orders))
                        if new_orders:
                            self.new_orders += new_orders
                            self.open_orders[order.sid] += new_orders
                            for o in new_orders:
                                self.orders[o.id] = o
                            # self.new_orders.append(order)

                        order.dt = txn.dt

        return transactions, commissions, closed_orders

    def close_existing_brackets(self, asset, amount):
        brackets = [b for b in self.open_orders[asset] if b.base_order_id]

        # sort by txn_price and walk
        base_ids = [b.base_order_id for b in brackets]
        base_ids = list(set(base_ids))

        base_orders = [self.orders[id] for id in base_ids]
        base_orders = sorted(base_orders,
                             key=lambda i: i.txn_price)

        for b in base_orders:
            current_amount = b.tp_order.amount
            if (amount > 0) != (current_amount > 0):
                # The reverse order amount isn't reverse at all.
                # Just create additional tp/sl, please
                break
            if abs(amount) <= abs(current_amount):
                # the reverse order partially closes existing tp/sl orders
                if b.tp_order:
                    b.tp_order.partially_cancel(current_amount-amount)
                if b.sl_order:
                    b.sl_order.partially_cancel(current_amount-amount)
                amount = 0
                break
            else:
                # the reverse order closes this entire existing tp/sl order
                # pair, and walks on to the next tp/sl
                amount -= current_amount
                b.tp_order.partially_cancel(current_amount)
                b.sl_order.partially_cancel(current_amount)

        return amount
