from zipline.finance.trading import TradingEnvironment
from zipline.data.loader import load_market_data
from zipline.utils.calendars import get_calendar
from zipline.assets import AssetDBWriter
from ..assets import AssetFinder


class BernoullioTradingEnvironment(TradingEnvironment):

    def __init__(self,
                 load=None,
                 bm_symbol='^GSPC',
                 exchange_tz="US/Eastern",
                 trading_calendar=None,
                 engine=None):

        self.bm_symbol = bm_symbol
        if not load:
            load = load_market_data

        if not trading_calendar:
            trading_calendar = get_calendar("NYSE")

        self.benchmark_returns, self.treasury_curves = load(
            trading_calendar.day,
            trading_calendar.schedule.index,
            self.bm_symbol,
        )

        self.exchange_tz = exchange_tz

        self.engine = engine

        if engine is not None:
            AssetDBWriter(engine).init_db()
            self.asset_finder = AssetFinder(engine)
        else:
            self.asset_finder = None
