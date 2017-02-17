import pytest
import pandas as pd
import pytz
import datetime
from . import resample


def test_bid_ask_to_ohlc():
    path = 'fixtures/bid_ask.csv'
    df = resample.bid_ask_to_ohlc(path)
    assert set(df.columns) == set(['open', 'high', 'low', 'close', 'volume'])
    assert df.ix[0, 'volume'] > 1
    assert df.index.tz.__str__() == 'UTC'
    assert df.index[0] == datetime.datetime(2016, 7, 29, 20, 50, 00, tzinfo=pytz.UTC)


def test_range_bars():
    expected = pd.read_csv("fixtures/range_3pips_for_m1_head.csv")['expected']
    candles = pd.read_csv("fixtures/m1.csv",
                          names=['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'volume'],
                          parse_dates=[[1, 2]])
    candles = candles[['open', 'high', 'low', 'close']]

    range_candles = resample.range_bars(candles.close, pips=3, pip_size=1e-4)

    assert (expected.iloc[0:10] == range_candles[0:10]).all()


def test_collapse():
    expected = pd.read_csv("fixtures/collapsed_range_3pips_for_m1_head.csv")['expected']
    candles = pd.read_csv("fixtures/m1.csv",
                          names=['symbol', 'date', 'time', 'open', 'high', 'low', 'close', 'volume'],
                          parse_dates=[[1, 2]])
    candles = candles[['open', 'high', 'low', 'close']]

    range_candles = resample.range_bars(candles.close, pips=3, pip_size=1e-4)
    collapsed = resample.collapse(range_candles[0:14])

    assert (expected == collapsed).all()
