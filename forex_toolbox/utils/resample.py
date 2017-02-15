"""
Convert other format into ohlc bars
"""
import pandas as pd


def bid_ask_to_ohlc(path):
    """
    CSV downloaded from truefx comes in the following format:
        EUR/USD,20160729 20:59:56.418,1.11712,1.11781
        EUR/USD,20160729 20:59:56.421,1.11697,1.11781
        EUR/USD,20160729 20:59:56.752,1.11696,1.11799
    This takes a file like above, resamples into minute ohlc bars

    Parameters
    ----------
    path : str
        Path to bid/ask csv data

    Returns
    -------
    ohlc     : the dataframe containing minute bar data
    """
    df = pd.read_csv(path,
                     header=None,
                     names=['name', 'datetime', 'bid', 'ask'],
                     parse_dates=[1])
    df.set_index('datetime', inplace=True)
    df['mid'] = (df['bid']*100000 + df['ask']*100000) // 2
    ohlcv = df.mid.resample('1Min').ohlc()
    ohlcv['volume'] = df.mid.resample('1Min').count()
    ohlcv.index = ohlcv.index.tz_localize('UTC')
    return ohlcv


def range_bars(prices, pips=5, pip_size=1e-4):
    int_prices = (prices / pip_size).round(0).astype(int)
    range_bars = [int_prices.loc[0]]

    for price in int_prices:
        change = price - range_bars[-1]
        while change > pips:
            range_bars.append(range_bars[-1] + pips + 1)
            change -= pips - 1
        while change < -pips:
            range_bars.append(range_bars[-1] - pips - 1)
            change += pips + 1

    return pd.Series(range_bars) * pip_size
