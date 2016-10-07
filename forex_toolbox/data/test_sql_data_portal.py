import os
import numpy as np
import pytest
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from .sql_data_portal import SqlMinuteReader, SqlDataPortal
from zipline_extension import ForexCalendar
from zipline_extension.assets import AssetFinder
from zipline_extension.finance.trading import BernoullioTradingEnvironment

from .oanda_minute_price_ingest import OandaMinutePriceIngest

from zipline import TradingAlgorithm
from zipline.finance.trading import SimulationParameters

# for the test algo
from zipline.api import sid


@pytest.fixture
def db_url():
    return os.environ.get('DATABASE_URL',
                          'postgres://postgres:password@localhost:5435/test')


@pytest.fixture
def engine(db_url):
    return create_engine(db_url, echo=False)


@pytest.fixture
def asset_finder(engine):
    return AssetFinder(engine)


def portal(reader, asset_finder):
    return SqlDataPortal(minute_reader=reader,
                         asset_finder=asset_finder)


def trading_env(engine):
    return BernoullioTradingEnvironment(engine=engine)


@pytest.fixture
def calendar():
    start = pd.Timestamp(datetime(2016, 9, 2))
    end = pd.Timestamp(datetime(2016, 9, 30, 23, 59))
    return ForexCalendar(start, end)


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


def test_sql_data_portal(db_url, asset_finder, calendar, candles):

    # with requests_mock.mock() as m:
        # ingest = OandaMinutePriceIngest(db_url)
        # m.get(ingest.url(), json=candles)
        # ingest.run("EUR_USD", calendar.schedule.index[-1])

    def initialize(context):
        context.has_price = False
        context.has_history = False
        context.i = 0

    def handle_data(context, data):
        if context.has_price is False and \
               data[37]['close'] is not np.NaN and \
               data[37]['volume'] is not np.NaN:
            context.has_price = True

        if context.has_history is False:
            if context.i > 20:
                hist = data.history(sid(37), ['close', 'open'], 20, '1m')
                if hist is not None:
                    context.has_history = True
            else:
                context.i += 1

    def analyze(context, perf):
        assert context.has_price
        assert context.has_history

    algo = TradingAlgorithm(initialize=initialize,
                            handle_data=handle_data,
                            trading_calendar=calendar,
                            analyze=analyze,
                            env=trading_env(engine(db_url)),
                            sim_params=SimulationParameters(
                                start_session=calendar.schedule.index[0],
                                end_session=calendar.schedule.index[-2],
                                capital_base=100,
                                data_frequency='minute',
                                emission_rate='minute',
                                trading_calendar=calendar
                            ))
    reader = SqlMinuteReader(db_url)
    algo.run(portal(reader, asset_finder))
