import os
import oandapy


class OandaPricesStream(oandapy.Streamer):
    """
    A long running process that uses oanda's HTTP Streaming API (see http://developer.oanda.com/rest-live/streaming/)
    to aggregate bid/ask ticks into minute OHLCV bar, and saved with minute_bar_writer.


    For live streaming, set the following env vars:
        - OANDA_ENV = practice
        - OANDA_ACCOUNT_IDJ = streaming_account_id
        - OANDA_ACCESS_TOKEN = xxx

    Live/backtest algos can access the persisted price history.

    """

    def __init__(self, environ, asset_db_writer,
                 minute_bar_writer, output_dir):

        self.environ = environ
        self.asset_db_writer = asset_db_writer
        self.minute_bar_writer = minute_bar_writer
        self.output_dir = output_dir

        if self.environ is None:
            self.environ = os.environ
        token = self.environ.get("OANDA_ACCESS_TOKEN", "practice")
        environment = self.environ.get("OANDA_ENV", "practice")

        super(OandaPricesStream, self).__init__(access_token=token,
                                                environment=environment)

    def run(self):
        super(OandaPricesStream, self).run('v1/prices', {'ignore_heartbeat': True})

    def stream_url(self):
        return '%s/%s' % (self.api_url, 'v1/prices')

    def on_success(self, data):
        """
        Aggregates tick data into a minute bar,
        then write with minute_bar_writer.

        Parameters
        ----------
        - data : response dict, loaded from stream response json
        """
        pass

    def on_error(self, data):
        """
        Backoff and try again
        """
        pass
