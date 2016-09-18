import pytest
from .mock_broker import Client

from .execution import (
        BracketedLimitOrder,
        BracketedStopOrder,
        BracketedMarketOrder
)
from zipline.finance.execution import (
        LimitOrder,
        MarketOrder,
        StopLimitOrder,
        StopOrder,
)


@pytest.fixture
def client():
    return Client(None, None, None)


def test__convert_order_params_for_blotter(client):
    assert client.convert_order_params_for_blotter(None, None, None, None).__class__ == MarketOrder

    assert client.convert_order_params_for_blotter(1, None, None, None).__class__ == LimitOrder

    assert client.convert_order_params_for_blotter(None, 2, None, None).__class__ == StopOrder

    assert client.convert_order_params_for_blotter(1, 2, None, None).__class__ == StopLimitOrder

    assert client.convert_order_params_for_blotter(1, 2, 3, None).__class__ == BracketedLimitOrder

    assert client.convert_order_params_for_blotter(1, 2, 3, 4).__class__ == BracketedLimitOrder

    assert client.convert_order_params_for_blotter(None, 2, 3, 4).__class__ == BracketedStopOrder

    assert client.convert_order_params_for_blotter(None, None, 3, 4).__class__ == BracketedMarketOrder
