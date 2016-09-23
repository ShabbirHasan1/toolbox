import os
import pytest
from datetime import datetime, timedelta
import pytz
import pandas as pd
from . import bid_ask_stream
import zipline
from ..zipline_extension.calendars.exchange_calendar_forex import ForexCalendar
from zipline.data.bundles import register
from zipline.utils.calendars import register_calendar


@pytest.mark.skip("need to download csv data zip from truefx and put into fixtures/stream")
def test_ingest():
    os.environ['DATA_START'] = '2016-06-01'
    os.environ['DATA_END'] = '2016-07-29'
    os.environ['BID_ASK_STREAM_CSV_FOLDER'] = 'fixtures/stream'

    register_calendar('forex', ForexCalendar(), force=True)
    register('bid_ask_stream',
             bid_ask_stream.ingest,
             calendar='forex',
             start_session=pd.Timestamp(os.environ.get("DATA_START"), tz='utc'),
             end_session=pd.Timestamp(os.environ.get("DATA_END"), tz='utc'),
             minutes_per_day=1440)

    zipline.data.bundles.ingest('bid_ask_stream', show_progress=True)
    assert True


def test_df_iloc():
    metadata = pd.DataFrame({
        'start_date': [ datetime.today() ],
        'end_date': [ datetime.today() + timedelta(days=1) ],
        'auto_close_date': [ datetime.today() + timedelta(days=2) ],
        'exchange': ['forex'],
        'symbol': ['test']
        })
    assert metadata.iloc[0]["start_date"] < metadata.iloc[0]["end_date"]
    metadata.ix[0, "start_date"] = datetime.today() + timedelta(days=3)
    assert metadata.iloc[0]["start_date"] > metadata.iloc[0]["end_date"]


def test_max_with_tz():
    t = pd.Timestamp.max.replace(tzinfo=pytz.UTC)
    today = datetime.today().replace(tzinfo=pytz.UTC)
    assert t > today
