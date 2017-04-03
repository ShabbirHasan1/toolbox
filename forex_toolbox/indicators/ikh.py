"""
Implements Ichimoku Kinyo Hyo indicator
"""

import pandas as pd


def lines_data(price, turn=9, base=26, lag_span=52, displacement=26):
    data = pd.DataFrame({'price': price}, index=price.index)
    base_roll = price.rolling(base)
    data['base'] = (base_roll.min() + base_roll.max()) / 2.0

    turn_roll = price.rolling(turn)
    data['turn'] = (turn_roll.min() + turn_roll.max()) / 2.0

    data['lag'] = data.price.shift(-displacement)
    data['cloud1'] = ((data.base + data.turn)/2).shift(displacement)
    cloud2_roll = price.rolling(lag_span)
    data['cloud2'] = ((cloud2_roll.min() + cloud2_roll.max())/2).shift(displacement)
    return data
