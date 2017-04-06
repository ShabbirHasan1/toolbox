"""
Implements Ichimoku Kinyo Hyo indicator
"""
import pytest
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

def mark_signal(data, max_cloud_thickness=1.5):
    data['buy'] = 0
    data['sell'] = 0
    # below_cloud = (data.price <= data.cloud1) & (data.price <= data.cloud2)
    # above_cloud = (data.price >= data.cloud1) & (data.price >= data.cloud2)
    thin_cloud  = (data.cloud1 - data.cloud2).abs() <= max_cloud_thickness
    within_cloud  = ((data.cloud1 - data.price) >= 0) != ((data.cloud2 - data.price) >= 0)

    real_turn_up = data.turn < data.price
    real_turn_down = data.turn > data.price

    curr_period_base = data.price - data.base
    prev_period_base = curr_period_base.shift(1)
    cross_up_base = (prev_period_base < 0) & (curr_period_base > 0)
    cross_down_base = (prev_period_base > 0) & (curr_period_base < 0)

    curr_period_turn = data.price - data.turn
    prev_period_turn = curr_period_turn.shift(1)
    cross_up_turn = (prev_period_turn < 0) & (curr_period_turn > 0)
    cross_down_turn = (prev_period_turn > 0) & (curr_period_turn < 0)

    data.loc[within_cloud & real_turn_up & cross_up_turn, 'buy'] = 1
    data.loc[within_cloud & real_turn_down & cross_down_turn, 'sell'] = 1
    data.loc[within_cloud & real_turn_up & cross_up_base, 'buy'] = 1
    data.loc[within_cloud & real_turn_down & cross_down_base, 'sell'] = 1
    return data

