import os
import json

inverse_ohlc_ratio = None


def sid(symbol):
    """
    Returns the arbitrary id assigned to the instrument
    symbol.

        See broker/oanda_instruments.json

        Return
    ------
    sid : int
    """
    global sym_sid_map
    if sym_sid_map is None:
        load_instruments_info()
    return sym_sid_map[symbol]


def symbol(sid):
    global sid_sym_map
    if sid_sym_map is None:
        load_instruments_info()
    return sid_sym_map[sid]


def display_name(sid):
    global sid_name_map
    if sid_name_map is None:
        load_instruments_info()
    return sid_name_map[sid]


def multiplier(instrument):
    global ohlc_ratio
    if ohlc_ratio is None:
        load_instruments_info()
    return ohlc_ratio[instrument]


def float_multiplier(sid):
    global inverse_ohlc_ratio
    if inverse_ohlc_ratio is None:
        load_instruments_info()
    return inverse_ohlc_ratio[sid]


def load_instruments_info():
    global sym_sid_map
    global sid_sym_map
    global sid_name_map
    global ohlc_ratio
    global inverse_ohlc_ratio
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open('{}/../broker/oanda_instruments.json'.format(dir_path)) as data_file:
        instruments  = json.load(data_file)
        sid_sym_map  = {i['sid']: i['instrument'] for i in instruments}
        sym_sid_map  = {i['instrument']: i['sid'] for i in instruments}
        sid_name_map = {i['sid']: i['displayName'] for i in instruments}
        ohlc_ratio   = {i['instrument']: int(100 * 1 / float(i['pip'])) for i in instruments}
        inverse_ohlc_ratio = {i['sid']: float(i['pip'])/100.0 for i in instruments}
