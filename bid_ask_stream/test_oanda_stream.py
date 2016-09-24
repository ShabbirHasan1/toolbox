import mock
import pytest

from .oanda_stream import OandaPricesStream

from zipline.assets import AssetDBWriter
from zipline.data.minute_bars import (
    BcolzMinuteBarReader,
    BcolzMinuteBarWriter
)

import httpretty


def mock_http_stream(url):
    def generate_stream(ticks):
        for t in ticks:
            yield t
    ticks = [
        '{"tick":{"instrument":"AUD_CAD","time":"2014-01-30T20:47:08.066398Z","bid":0.98114,"ask":0.98139}}\r\n',
        '{"tick":{"instrument":"AUD_CHF","time":"2014-01-30T20:47:08.053811Z","bid":0.79353,"ask":0.79382}}\r\n',
        '{"tick":{"instrument":"AUD_CHF","time":"2014-01-30T20:47:11.493511Z","bid":0.79355,"ask":0.79387}}\r\n',
        '{"heartbeat":{"time":"2014-01-30T20:47:11.543511Z"}}\r\n',
        '{"tick":{"instrument":"AUD_CHF","time":"2014-01-30T20:47:11.855887Z","bid":0.79357,"ask":0.79390}}\r\n',
        '{"tick":{"instrument":"AUD_CAD","time":"2014-01-30T20:47:14.066398Z","bid":0.98112,"ask":0.98138}}\r\n'
    ]
    httpretty.register_uri(httpretty.GET, url,
                           body=generate_stream(ticks),
                           streaming=True)


@httpretty.activate
def test_stream_mock():
    class MockOandaPricesStream(OandaPricesStream):
        def __init__(self, *args, **kwargs):
            super(MockOandaPricesStream, self).__init__(None, None, None, None, *args, **kwargs)
            self.count = 0

        def on_success(self, data):
            assert "tick" in data
            assert "heartbeat" not in data
            self.count += 1
            if self.count == 4:
                self.disconnect()

    streamer = MockOandaPricesStream()
    mock_http_stream(streamer.stream_url())
    streamer.run()
