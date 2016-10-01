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
    now = datetime.now()
    m = now - timedelta(seconds=now.second,
                        microseconds=now.microsecond)
    return {
        "instrument": "EUR_USD",
        "granularity": "M1",
        "candles": [
            {
                "time": (m-timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "openMid": 1.36803,
                "highMid": 1.368125,
                "lowMid": 1.364275,
                "closeMid": 1.364315,
                "volume": 28242,
                "complete": True
            },
            {
                "time": (m-timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "openMid": 1.36803,
                "highMid": 1.368125,
                "lowMid": 1.364275,
                "closeMid": 1.365315,
                "volume": 28242,
                "complete": True
            },
            {
                "time": m.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                "openMid": 1.36532,
                "highMid": 1.366445,
                "lowMid": 1.35963,
                "closeMid": 1.3613,
                "volume": 30487,
                "complete": False
            }
        ]
    }


@pytest.fixture
def db_url():
    return 'postgres://postgres:password@localhost:5435/test'


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
        res = c.execute("SELECT count(*) from minute_bars_37") # 37 is the sid for eur-usd
        assert res.fetchone()[0] > 2
        c.close()

        reader = AssetFinder(eng)
        eurusd = reader.retrieve_asset(37)
        assert eurusd.symbol == "EUR_USD"
