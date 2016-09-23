"""
Reads csv file of streamed bid and ask prices, then ingests into
zipline data.bundle
- Set BID_ASK_STREAM_CSV_FOLDER=path, where path contains the csv data as seen
in _minute_iter docstring
"""
import os
import zipfile
import pandas as pd
import numpy as np
from ..utils import resample
from zipline.utils.cli import maybe_show_progress


def ingest(environ,
           asset_db_writer,
           minute_bar_writer,
           daily_bar_writer,
           adjustment_writer,
           calendar,
           start_session,
           end_session,
           cache,
           show_progress,
           output_dir):

    path = environ.get('BID_ASK_STREAM_CSV_FOLDER')
    instruments = os.listdir(path)  # get ["EURSD", "AUDUSD"]

    # init metadata
    metadata = pd.DataFrame(np.empty(len(instruments), dtype=[
        ('start_date',      'datetime64[ns]'),
        ('end_date',        'datetime64[ns]'),
        ('auto_close_date', 'datetime64[ns]'),
        ('exchange',        'object'),
        ('symbol',          'object'),
        ('asset_name',      'object'),
        ]))
    metadata['start_date'] = metadata.start_date.dt.tz_localize('UTC')
    metadata['end_date'] = metadata.end_date.dt.tz_localize('UTC')
    metadata['auto_close_date'] = metadata.auto_close_date.dt.tz_localize('UTC')

    # Fix calendar
    calendar.schedule['market_open'] = calendar.schedule.market_open.dt.tz_localize('UTC')
    calendar.schedule['market_close'] = calendar.schedule.market_close.dt.tz_localize('UTC')

    def _minute_iter(path):
        """ Yields (sid, dataframe) for ingesting, while updating
        metadata as a closure

        Parameters
        ----------
        path : str
            The path to a folder containing sub folder of instruments,
            which in turn contain ohlc directory. For example:
            /path:
              ├── EURUSD
              │   ├── EURUSD-2016-06.zip
              │   └── EURUSD-2016-07.zip
              └── GBPUSD
                  ├── GBPUSD-2016-06.zip
                  └── GBPUSD-2016-07.zip
        Returns
        -------
        Yield (sid, dataframe)

        Note
        ----
        sid is index of insturment folder in the path. No special meaning.
        """
        instruments = os.listdir(path)  # get ["EURSD", "AUDUSD"]
        for index, name in enumerate(instruments):
            metadata.ix[index] = None, None, None, 'forex', name, name
            current_dir = os.path.join(path, name)

            zips = filter(lambda x: ".zip" in x, os.listdir(current_dir))
            for z in zips:
                zfile = zipfile.ZipFile(os.path.join(current_dir, z), 'r')
                zfile.extractall(current_dir)

            csvs = filter(lambda x: ".csv" in x, os.listdir(current_dir))
            with maybe_show_progress(
                    csvs,
                    show_progress,
                    label='Ingesting csv stream for %s: ' % name):
                for minute_csv in csvs:
                    ohlc = resample.bid_ask_to_ohlc(os.path.join(current_dir, minute_csv))

                    # Keep metadata updated
                    if metadata.ix[index, "start_date"] is pd.NaT or metadata.start_date.ix[index] > ohlc.index[0]:
                        metadata.ix[index, "start_date"] = ohlc.index[0]
                    if metadata.ix[index, "end_date"] is pd.NaT or metadata.end_date.ix[index] < ohlc.index[-1]:
                        metadata.ix[index, "end_date"] = ohlc.index[-1]
                        metadata.ix[index, "auto_close_date"] = ohlc.index[-1] + pd.Timedelta(days=1)
                    yield index, ohlc

    minute_bar_writer.write(_minute_iter(path), show_progress)
    asset_db_writer.write(equities=metadata)

    adjustment_writer.write()
