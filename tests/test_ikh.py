import pytest
import pandas as pd
from ..forex_toolbox.indicators.ikh import lines_data


def test_lines_data():
    data = pd.read_csv("fixtures/ikh_price.csv")
    total_periods = len(data)
    ikh_lines = lines_data(data.price)
    assert ikh_lines.base.iloc[-1] == pytest.approx(1.28555)
    assert ikh_lines.turn.iloc[-1] == pytest.approx(1.28685)
    assert ikh_lines.lag.iloc[0] == pytest.approx(1.3175)
    assert ikh_lines.cloud1.iloc[-1] == pytest.approx(1.284875)
    assert ikh_lines.cloud2.iloc[-1] == pytest.approx(1.28595)
    assert set(ikh_lines.keys()) == set(['price', 'base', 'turn', 'lag', 'cloud1', 'cloud2'])
    assert len(ikh_lines.base.dropna()) == total_periods - 26 + 1
    assert len(ikh_lines.turn.dropna()) == total_periods - 9 + 1
    assert len(ikh_lines.lag.dropna()) == total_periods - 26

    assert len(ikh_lines.cloud1.dropna()) == total_periods - 52 + 1 # 26 periods ahead
    assert len(ikh_lines.cloud2.dropna()) == total_periods - 52 + - 26 + 1
