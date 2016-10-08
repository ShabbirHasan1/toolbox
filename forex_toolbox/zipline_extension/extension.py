import os
import bid_ask_stream
from zipline.data.bundles import register
from zipline.utils.calendars import register_calendar
from .calendars.exchange_calendar_forex import ForexCalendar
import pandas as pd


def override_nyse():
    # Override the NYSE calendar, because it's everywhere. Not proud.
    register_calendar('NYSE', ForexCalendar(), force=True)
    print("Registered NYSE as forex calendar")


def register_bid_ask_stream():
    """
    Register a bundle, so we can read csv file and ingests into zipline
    databundle. Example:
        - BID_ASK_STREAM_CSV_FOLDER=data/eur_usd_m1.csv zipline ingest -b bid_ask_stream
    """
    register('bid_ask_stream', bid_ask_stream.ingest,
             start_session=pd.Timestamp(os.environ.get("DATA_START"), tz='utc'),
             end_session=pd.Timestamp(os.environ.get("DATA_END"), tz='utc'),
             calendar='NYSE', minutes_per_day=1440)
    print("Registered bundle bid_ask_stream")
