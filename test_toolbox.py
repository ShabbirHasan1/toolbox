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
            context.broker = Client(context, "backtest", context.asset_finder)

        def handle_data(context, data):
            # Asset 'A' has price increasing linearly from 10 to 260
            # Asset 'B' has price increasing linearly from 11 to 261
            # Asset 'C' has price increasing linearly from 12 to 262
            a, b, c = symbol('A'), symbol('B'), symbol('C')
            if not hasattr(context, "ordered"):
                context.ordered = False
            if not context.ordered:
                context.ordered = True
                context.broker.create_order(a, 100, limit=270)  # remain open
                context.broker.create_order(a, 100, stop=20)   # filled
                """
                context.broker.create_order(a, 100, limit=100,
                                            take_profit=102,
                                            stop_loss=98)   # filled, profitted

                context.broker.create_order(b, -100, limit=200,
                                            take_profit=198,
                                            stop_loss=202)   # filled, loss

                context.broker.create_order(b, -100, limit=200,
                                            take_profit=198,
                                            stop_loss=302)   # filled, remain open

                context.broker.create_order(c, -100, limit=200,
                                            take_profit=188,
                                            stop_loss=212)   # filled, early closed

            if data[c].price == 205:
                context.broker.create_order(c, 100, limit=200,
                                            take_profit=212,
                                            stop_loss=188)   # closes earlier bracket order
                                            """

        def analyze(context, perf):
            pytest.set_trace()
            assert len(context.broker.open_orders[symbol('A')]) == 1

        algo = TradingAlgorithm(initialize=initialize,
                                handle_data=handle_data,
                                analyze=analyze,
                                sim_params=self.sim_params,
                                env=self.env)
        algo.run(self.data_portal)
