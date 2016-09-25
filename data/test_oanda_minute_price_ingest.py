import os
import pytest
import requests_mock
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime, timedelta

from .oanda_stream import OandaMinutePriceIngest
from ..zipline_extension.calendars.exchange_calendar_forex import ForexCalendar

from zipline.assets import AssetDBWriter
from zipline.data.minute_bars import (
    BcolzMinuteBarReader,
    BcolzMinuteBarWriter
)


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
def data_dir():
    version = OandaMinutePriceIngest.VERSION
    root = os.environ.get("ZIPLINE_ROOT", "/home/.zipline")
    return '{}/data/oanda/{}-{}/ohlcv.db' \
            .format(root, 'practice', version)


@pytest.fixture
def a_writer(data_dir):
    return AssetDBWriter(data_dir)


@pytest.fixture
def m_writer(data_dir):
    end_session = datetime.now() + timedelta(days=7)
    cal = ForexCalendar(end=end_session)
    return BcolzMinuteBarWriter(
        data_dir,
        calendar=cal,
        start_session=pd.Timestamp('1990-01-01', tz='UTC'),
        end_session=end_session,
        minutes_per_day=1440)


def test_oanda_prices_ingest(candles, data_dir):
    ingest = OandaMinutePriceIngest(data_dir)

    with requests_mock.mock() as m:
        m.get(ingest.url(), json=candles)

        ingest.run("AUD_CAD")

        eng = create_engine('sqlite:///{}'.format(data_dir))
        c = eng.connect()
        for row in c.execute("SELECT * from minute_bars_2"):
            print(row)
