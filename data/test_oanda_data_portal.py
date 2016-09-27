import os
import numpy as np
import pytest
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine

from .oanda_data_portal import OandaMinuteReader, OandaDataPortal
from ..zipline_extension import ForexCalendar
from .oanda_minute_price_ingest import OandaMinutePriceIngest

from zipline.assets import AssetFinder
from zipline import TradingAlgorithm
from zipline.finance.trading import SimulationParameters
from zipline.finance.trading import TradingEnvironment


@pytest.fixture
def ohlcv_path():
    version = OandaMinutePriceIngest.VERSION
    root = os.environ.get("ZIPLINE_ROOT", "/home/.zipline")
    return '{}/data/oanda/{}-{}/ohlcv.db' \
            .format(root, 'practice', version)


@pytest.fixture
def assets_path():
    version = OandaMinutePriceIngest.VERSION
    root = os.environ.get("ZIPLINE_ROOT", "/home/.zipline")
    return '{}/data/oanda/{}-{}/assets.db' \
            .format(root, 'practice', version)


@pytest.fixture
def engine(assets_path):
    return create_engine('sqlite:///{}'.format(assets_path), echo=True)


@pytest.fixture
def asset_finder(engine):
    return AssetFinder(engine)


@pytest.fixture
def test_portal(ohlcv_path, calendar, asset_finder):
    reader = OandaMinuteReader(ohlcv_path, calendar)
    return OandaDataPortal(minute_reader=reader,
                           asset_finder=asset_finder)


def trading_env(engine, calendar):
    return TradingEnvironment(asset_db_path=engine, trading_calendar=calendar)


@pytest.fixture
def calendar():
    now = pd.Timestamp.now(tz='utc')
    now = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = now - timedelta(days=7)
    return ForexCalendar(start, now-timedelta(days=5))


def test_oanda_data_portal(test_portal, ohlcv_path, assets_path, calendar):

    ingest = OandaMinutePriceIngest(ohlcv_path, assets_path)
    ingest.run("EUR_USD", calendar.schedule.index[-1])

    def initialize(context):
        context.has_price = False

    def handle_data(context, data):
        if context.has_price is False and \
               data[37]['close'] is not np.NaN and \
               data[37]['volume'] is not np.NaN:
            context.has_price = True

    def analyze(context, perf):
        assert context.has_price

    algo = TradingAlgorithm(initialize=initialize,
                            handle_data=handle_data,
                            env=trading_env(engine(assets_path), calendar),
                            sim_params=SimulationParameters(
                                start_session=calendar.schedule.index[-1],
                                end_session=calendar.schedule.index[-1],
                                capital_base=100,
                                data_frequency='minute',
                                emission_rate='minute',
                                trading_calendar=calendar
                            ))
    algo.run(test_portal)
