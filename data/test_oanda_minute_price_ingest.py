import os
import pandas as pd
from ..zipline_extension import ForexCalendar
import pytest
import requests_mock
from sqlalchemy import create_engine
from datetime import datetime, timedelta

from .oanda_minute_price_ingest import OandaMinutePriceIngest

from ..zipline_extension.assets import AssetFinder


@pytest.fixture
def candles():
    df = pd.read_csv("fixtures/m1.csv",
                     header=None,
                     names=['symbol', 'date', 'time', 'openMid', 'highMid', 'lowMid', 'closeMid', 'volume'],
                     parse_dates=[[1, 2]])
    df['complete'] = True
    df['time'] = df['date_time'].apply(lambda x: x.strftime("%Y-%m-%dT%H:%M:%S.%fZ"))
    df = df.drop('date_time', 1)
    df = df.drop('symbol', 1)

    return {
        "instrument": "EUR_USD",
        "granularity": "M1",
        "candles": df.to_dict("records")}


@pytest.fixture
def db_url():
    return os.environ.get('DATABASE_URL',
                          'postgres://postgres:password@localhost:5435/test')


@pytest.fixture
def trading_calendar():
    start = pd.Timestamp('2016-09-02', tz='utc')
    return ForexCalendar(start)


def test_oanda_prices_ingest(candles,
                             trading_calendar,
                             db_url):
    ingest = OandaMinutePriceIngest(db_url)

    with requests_mock.mock() as m:
        m.get(ingest.url(), json=candles)

        ingest.run("EUR_USD")

        eng = create_engine(db_url)
        c = eng.connect()
        res = c.execute('SELECT count(*) from "minute_bars_EUR_USD"')
        assert res.fetchone()[0] == 30965
        c.close()

        reader = AssetFinder(eng)
        eurusd = reader.retrieve_asset(37)
        assert eurusd.symbol == "EUR_USD"
