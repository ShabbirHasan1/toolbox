import os
import pytest
import pytz
import datetime
from . import resample
import pandas as pd

def test_bid_ask_to_ohlc():
    path = 'fixtures/bid-ask.csv'
    df = resample.bid_ask_to_ohlc(path)
    assert set(df.columns) == set(['open', 'high', 'low', 'close', 'volume'])
    assert df.ix[0, 'volume'] > 1
    assert df.index.tz.__str__() == 'UTC'
    assert df.index[0] == datetime.datetime(2016,7,29,20,50,00, tzinfo=pytz.UTC)

