""" Meta wrapper for oanda apis
See http://developer.oanda.com/rest-live

TODO: Benchmark latency
"""
import pytest
import os
import json
import oandapy

import logging


class Oanda(oandapy.oandapy.API):
    PRECISION = {'EUR_USD': '%.5f',
                 'USD_JPY': '%.3f',
                 'EUR_JPY': '%.3f',
                 'AUD_USD': '%.5f',
                 'GBP_USD': '%.5f'}

    def __init__(self, id):
        self.id = id
        super(Oanda, self).__init__(environment=os.getenv("OANDA_ENV", "practice"),
                                 access_token=os.getenv("OANDA_ACCESS_TOKEN", "xxx"))

        self.oanda = oandapy.API(environment=os.getenv("OANDA_ENV", "practice"),
                                 access_token=os.getenv("OANDA_ACCESS_TOKEN", "xxx"))

    def get_account(self):
        params = {"account_id": self.id}
        response = self.oanda.get_account(**params)
        logging.info("#get_account params=%s response=%s" % (params, response))
        return response

    def create_order(self, instrument, amount, order_type='market',
                     price=None, lower_bound=None, upper_bound=None, expiry=None,
                     stop_loss=None, take_profit=None, trailling=None):
        """
        Creates an order with Oanda rest api.

        Default expiry is 1 month from now, for limit, stop
        and marketIfTouch order types.

        Return
        ------
        order_id: string
            Oanda order id string
        """

        if amount < 0:
            side = "sell"
        else:
            side = "buy"

        instrument_string = ""
        if type(instrument) is str:
            instrument_string = instrument
        else:
            instrument_string = instrument.symbol
        params = {"account_id": self.id,
                  "instrument": instrument_string,
                  "units":      abs(amount),
                  "side":       side,
                  "type":       order_type}


        if expiry is not None:
            if type(expiry) is str:
                expiry_string = expiry
            else:
                expiry_string = expiry.strftime("%Y-%m-%dT%H:%M:%S")
            params["expiry"] = expiry_string

        precision = Oanda.PRECISION[instrument_string]
        if price and order_type != 'market':
            params["price"] = precision % price

        if lower_bound:
            params["lowerBound"] = precision % lower_bound

        if upper_bound:
            params["upperBound"] = precision % upper_bound

        if stop_loss:
            params["stopLoss"] = precision % stop_loss

        if take_profit:
            params["takeProfit"] = precision % take_profit

        if trailling:
            params["trailingStop"] = trailling

        try:
            response = self.oanda.create_order(**params)
            logging.info("#create_order params=%s response=%s" % (params, response))
            return response["tradeOpened"]["id"]
        except oandapy.exceptions.OandaError as e:
            logging.exception(e)
            return None

    def close_position(self, instrument):
        try:
            response = self.oanda.close_position(self.id, instrument)
            logging.info("#close_position params=%s response=%s" % ({'id': self.id, 'instrument': instrument}, response))
            return response
        except oandapy.exceptions.OandaError as e:
            logging.exception(e)
            return None

    def get_history(self, instrument, count=500, resolution="m1", end=None, candleFormat="midpoint"):
        params = {"instrument": instrument.upper(),
                  "count": count,
                  "end": end,
                  "granularity": resolution.upper(),
                  "candleFormat": candleFormat}
        response = self.oanda.get_history(**params)
        return response["candles"]

    def get_position(self, instrument):
        params = {"instrument": instrument.upper(),
                  "account_id": self.id}
        try:
            return self.oanda.get_position(**params)
        except oandapy.OandaError as err:
            if 'Position not found' in str(err):
                return None
            else:
                raise err


