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
            context.broker = Client("backtest", context.asset_finder)

        def handle_data(context, data):
            # Asset 'A' has price increasing linearly from 10 to 260
            # Asset 'B' has price increasing linearly from 11 to 261
            # Asset 'C' has price increasing linearly from 12 to 262
            a = symbol('A')
            context.ordered = False
            if not context.ordered:
                context.broker.create_order(a, 100, limit=10.1,
                                            stop_loss=9.00, take_profit=11.00)
                context.ordered = True

        def analyze(context, perf):
            assert len(context.broker.open_orders) == 1

        algo = TradingAlgorithm(initialize=initialize,
                                handle_data=handle_data,
                                analyze=analyze,
                                sim_params=self.sim_params,
                                env=self.env)
        algo.run(self.data_portal)
