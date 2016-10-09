from datetime import timedelta
from zipline import TradingAlgorithm
from zipline.testing.fixtures import (
    WithDataPortal,
    WithLogger,
    WithSimParams,
    ZiplineTestCase,
)

from .simu_broker import (
    SimuBroker,
    convert_order_params_for_blotter,
    time_delta
)

from ..zipline_extension.execution import (
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

from zipline.api import symbol


class SimuBrokerTestCase(WithDataPortal,
                         WithLogger,
                         WithSimParams,
                         ZiplineTestCase):

    def initialize(self, context):
        context.broker = SimuBroker(context)
        context.blotter = context.broker.blotter

    def test_take_profit(self):

        def handle_data(context, data):

            # Asset 'A' has price increasing linearly from 10 to 260
            a = symbol('A')
            if not hasattr(context, "ordered"):
                context.ordered = False
            if not context.ordered:
                context.ordered = True

                # Asset A filled, profit taken
                context.broker.create_order(a, 1, stop=100,
                                            take_profit=102,
                                            stop_loss=98)

        def analyze(context, perf):
            assert len(context.blotter.open_orders[symbol('A')]) == 0
            assert len(context.blotter.profit_orders[symbol('A')]) == 1
            assert len(context.blotter.loss_orders[symbol('A')]) == 0
            assert context.portfolio.pnl > 0
            assert context.portfolio.positions == {}

        algo = TradingAlgorithm(initialize=self.initialize,
                                handle_data=handle_data,
                                analyze=analyze,
                                sim_params=self.sim_params,
                                env=self.env)
        algo.run(self.data_portal)

    def test_stop_loss_then_new_order(self):
        def handle_data(context, data):
            # Asset 'B' has price increasing linearly from 11 to 261
            b = symbol('B')
            if not hasattr(context, "ordered"):
                context.ordered = False
            if not context.ordered:
                context.ordered = True

                # Asset B filled, loss stopped, then new position opened
                context.broker.create_order(b, -1, limit=200,
                                            take_profit=198,
                                            stop_loss=202)

                context.broker.create_order(b, -2, limit=200,
                                            take_profit=197,
                                            stop_loss=302)

        def analyze(context, perf):
            assert len(context.blotter.open_orders[symbol('B')]) == 2
            assert len(context.blotter.profit_orders[symbol('B')]) == 0
            assert len(context.blotter.loss_orders[symbol('B')]) == 1
            assert context.portfolio.positions[symbol('B')].amount == -2
            assert context.portfolio.positions_value < 0
            assert context.portfolio.pnl < 0

        algo = TradingAlgorithm(initialize=self.initialize,
                                handle_data=handle_data,
                                analyze=analyze,
                                sim_params=self.sim_params,
                                env=self.env)
        algo.run(self.data_portal)

    def test_partial_closing(self):

        def handle_data(context, data):
            # Asset 'C' has price increasing linearly from 12 to 262
            c = symbol('C')
            if not hasattr(context, "ordered"):
                context.ordered = False
            if not context.ordered:
                context.ordered = True

                # Asset C filled, new reverse order partially closes position
                context.broker.create_order(c, 2, stop=200,
                                            take_profit=332,
                                            stop_loss=132)   # filled, early closed

                context.broker.create_order(c, 2, stop=220,
                                            take_profit=335,
                                            stop_loss=135)   # filled, early closed

            if data[c].price == 222:
                # Asset C reverse order
                context.broker.create_order(c, -1,
                                            take_profit=112,
                                            stop_loss=388)   # closes earlier bracket order

        def analyze(context, perf):
            assert len(context.blotter.open_orders[symbol('C')]) == 4
            assert len(context.blotter.profit_orders[symbol('C')]) == 0
            assert len(context.blotter.loss_orders[symbol('C')]) == 0
            assert context.portfolio.positions[symbol('C')].amount == 3

            """ should have 50 profit at 222,
            50 tp at 232,
            100 tp at 235 for 'C' """

        algo = TradingAlgorithm(initialize=self.initialize,
                                handle_data=handle_data,
                                analyze=analyze,
                                sim_params=self.sim_params,
                                env=self.env)
        algo.run(self.data_portal)


def test_convert_order_params_for_blotter():
    assert convert_order_params_for_blotter(None, None, None, None, None).__class__ == BracketedMarketOrder

    assert convert_order_params_for_blotter(1, None, None, None, None).__class__ == BracketedLimitOrder

    assert convert_order_params_for_blotter(None, 2, None, None, None).__class__ == BracketedStopOrder

    assert convert_order_params_for_blotter(1, 2, None, None, None).__class__ == BracketedStopLimitOrder

    assert convert_order_params_for_blotter(1, 2, 3, None, None).__class__ == BracketedStopLimitOrder

    assert convert_order_params_for_blotter(1, 2, 3, 4, None).__class__ == BracketedStopLimitOrder

    assert convert_order_params_for_blotter(None, 2, 3, 4, None).__class__ == BracketedStopOrder

    assert convert_order_params_for_blotter(1, None, 3, 4, None).__class__ == BracketedLimitOrder

    assert convert_order_params_for_blotter(None, None, 3, 4, None).__class__ == BracketedMarketOrder


def test_time_delta():
    assert time_delta("M1") == timedelta(minutes=1)
    assert time_delta("M15") == timedelta(minutes=15)
    assert time_delta("S5") == timedelta(seconds=5)
