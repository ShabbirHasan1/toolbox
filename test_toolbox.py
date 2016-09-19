import pytest
from zipline import TradingAlgorithm
from zipline.testing.fixtures import (
    WithDataPortal,
    WithLogger,
    WithSimParams,
    ZiplineTestCase,
)

from .broker.mock import Client
from zipline.api import symbol


class ToolboxTestCase(WithDataPortal,
                      WithLogger,
                      WithSimParams,
                      ZiplineTestCase):

    def test_create_order(self):

        def initialize(context):
            context.blotter = Client(context, "backtest", context.asset_finder)

        def handle_data(context, data):
            # Asset 'A' has price increasing linearly from 10 to 260
            # Asset 'B' has price increasing linearly from 11 to 261
            # Asset 'C' has price increasing linearly from 12 to 262
            a, b, c = symbol('A'), symbol('B'), symbol('C')
            if not hasattr(context, "ordered"):
                context.ordered = False
            if not context.ordered:
                context.ordered = True

                context.blotter.create_order(a, 1, stop=100,
                                             take_profit=102,
                                             stop_loss=98)   # filled, profitted

                context.blotter.create_order(b, -1, limit=200,
                                             take_profit=198,
                                             stop_loss=202)   # filled, loss

                context.blotter.create_order(b, -100, limit=200,
                                             take_profit=198,
                                             stop_loss=302)   # filled, remain open

                context.blotter.create_order(c, 100, limit=200,
                                             take_profit=232,
                                             stop_loss=132)   # filled, early closed

                context.blotter.create_order(c, 100, limit=220,
                                             take_profit=235,
                                             stop_loss=135)   # filled, early closed

            if data[c].price == 222:
                context.blotter.create_order(c, -50,
                                             take_profit=112,
                                             stop_loss=288)   # closes earlier bracket order

        def analyze(context, perf):
            assert len(context.blotter.open_orders[symbol('A')]) == 0
            assert len(context.blotter.open_orders[symbol('B')]) == 1
            assert len(context.blotter.open_orders[symbol('C')]) == 0

            assert len(context.blotter.profit_orders[symbol('A')]) == 1
            assert len(context.blotter.profit_orders[symbol('B')]) == 0
            assert len(context.blotter.profit_orders[symbol('C')]) == 1

            assert len(context.blotter.loss_orders[symbol('A')]) == 0
            assert len(context.blotter.loss_orders[symbol('B')]) == 1
            assert len(context.blotter.loss_orders[symbol('C')]) == 0

            """ should have 50 profit at 222,
            50 tp at 232,
            100 tp at 235 for 'C' """

        algo = TradingAlgorithm(initialize=initialize,
                                handle_data=handle_data,
                                analyze=analyze,
                                sim_params=self.sim_params,
                                env=self.env)
        algo.run(self.data_portal)

