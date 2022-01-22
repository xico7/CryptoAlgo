import traceback
import copy

import requests
from pymongo.errors import ServerSelectionTimeoutError
from datetime import datetime
import data_staging as dts
from binance import AsyncClient, BinanceSocketManager
import MongoDB.DBactions as mongo
import asyncio
import logging
import time


class QueueOverflow(Exception):
    pass


logging.basicConfig(level=logging.INFO)

SP500_SYMBOLS_USDT_PAIRS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'LUNAUSDT',
                            'DOGEUSDT',
                            'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'TRXUSDT', 'BCHUSDT',
                            'ALGOUSDT',
                            'MANAUSDT', 'XLMUSDT', 'AXSUSDT', 'VETUSDT', 'FTTUSDT', 'EGLDUSDT', 'ATOMUSDT', 'ICPUSDT',
                            'FILUSDT',
                            'HBARUSDT', 'SANDUSDT', 'THETAUSDT', 'FTMUSDT',
                            'NEARUSDT', 'BTTUSDTXTZUSDT', 'XMRUSDT', 'KLAYUSDT', 'GALAUSDT', 'HNTUSDT', 'GRTUSDT',
                            'LRCUSDT']


class Cache:
    _cached_coins_volume = {}
    _cached_coins_moment_price = {}
    _cached_marketcap_coins_value = {}
    _cached_marketcap_sum = 0

    _cached_marketcap_latest_timestamp = 0
    _cached_marketcap_current_ohlc = {'t': 0, 'h': 0, 'o': 0, 'l': 999999999999999, 'c': 0}
    _cached_coins_current_ohlcs = {}

    _cached_marketcap_ohlc_data = {}
    _cached_coins_ohlc_data = {}

    _cached_aggtrade_data = {}

    @property
    def aggtrade_data(self):
        return self._cached_aggtrade_data

    @aggtrade_data.setter
    def aggtrade_data(self, value):
        if value == {}:
            self._cached_aggtrade_data = {}
            return

        coin_symbol = list(value.keys())[0]
        coin_data = list(value.values())[0]

        if coin_symbol not in self._cached_aggtrade_data:
            self._cached_aggtrade_data.update({coin_symbol: [coin_data]})
        else:
            self._cached_aggtrade_data[coin_symbol].append(coin_data)

    @property
    def coins_volume(self):
        return self._cached_coins_volume

    @coins_volume.setter
    def coins_volume(self, value):
        self._cached_coins_volume = value

    @property
    def coins_moment_price(self):
        return self._cached_coins_moment_price

    @coins_moment_price.setter
    def coins_moment_price(self, value):
        self._cached_coins_moment_price = value

    @property
    def marketcap_coins_value(self):
        return self._cached_marketcap_coins_value

    @marketcap_coins_value.setter
    def marketcap_coins_value(self, value):
        self._cached_marketcap_coins_value = value

    @property
    def marketcap_sum(self):
        return self._cached_marketcap_sum

    @marketcap_sum.setter
    def marketcap_sum(self, value):
        self._cached_marketcap_sum = value

    @property
    def marketcap_latest_timestamp(self):
        return self._cached_marketcap_latest_timestamp

    @marketcap_latest_timestamp.setter
    def marketcap_latest_timestamp(self, value):
        self._cached_marketcap_latest_timestamp = value

    @property
    def marketcap_current_ohlc(self):
        return self._cached_marketcap_current_ohlc

    @marketcap_current_ohlc.setter
    def marketcap_current_ohlc(self, value):
        self._cached_marketcap_current_ohlc = value

    @property
    def coins_current_ohlcs(self):
        return self._cached_coins_current_ohlcs

    @coins_current_ohlcs.setter
    def coins_current_ohlcs(self, value):
        self._cached_coins_current_ohlcs = value

    @property
    def marketcap_ohlc_data(self):
        return self._cached_marketcap_ohlc_data

    @marketcap_ohlc_data.setter
    def marketcap_ohlc_data(self, value):
        self._cached_marketcap_ohlc_data = value

    @property
    def coins_ohlc_data(self):
        return self._cached_coins_ohlc_data

    @coins_ohlc_data.setter
    def coins_ohlc_data(self, value):
        self._cached_coins_ohlc_data = value


coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/" \
                               "markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"

AGGTRADE_PYCACHE = 1000
CANDLESTICK_WS = "kline"
CANDLESTICKS_ONE_MINUTE_WS = f"@{CANDLESTICK_WS}_1m"
AGGREGATED_TRADE_WS = "@aggTrade"
OHLC_CACHE_PERIODS = 3  # This value will be 70.
REL_STRENGTH_PERIODS = OHLC_CACHE_PERIODS - 1  # This value will be 15. #TODO: if this is always OHLC-1 just make it equal to that
abc = []


async def binance_to_mongodb(multisocket_candle, candlestick_db, ta_lines_db, coin_ratio):
    time_counter = int(time.time())
    cache = Cache()
    COUNTER = 0
    async with multisocket_candle as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()

                if int(time.time()) > time_counter + 2 and cache.marketcap_latest_timestamp > 0:
                    time_counter += 2
                    cache.marketcap_current_ohlc = dts.update_current_marketcap_ohlc_data(
                        cache.marketcap_current_ohlc, cache.marketcap_latest_timestamp,
                        cache.marketcap_latest_timestamp)
                    if len(cache.marketcap_ohlc_data) == OHLC_CACHE_PERIODS:
                        marketcap_relative_atr = dts.calculate_relative_atr(cache.marketcap_ohlc_data)
                        for coin_ohlc_data in cache.coins_ohlc_data.items():
                            if len(coin_ohlc_data[1]) == OHLC_CACHE_PERIODS:
                                coin_rel_strength = dts.calculate_relative_strength(
                                    coin_ohlc_data[1], marketcap_relative_atr, cache.marketcap_ohlc_data)

                                abc.append(coin_rel_strength)

                    # TODO: insert in db RS,Volume,price

                    cache.coins_volume = {}

                if CANDLESTICK_WS in ws_trade['stream']:
                    cache.coins_current_ohlcs, cache.coins_ohlc_data, cache.marketcap_ohlc_data, cache.marketcap_latest_timestamp = \
                        await dts.update_ohlc_cached_values(cache.coins_current_ohlcs, ws_trade['data']['k'],
                                                            candlestick_db,
                                                            cache.coins_ohlc_data, cache.marketcap_ohlc_data,
                                                            cache.marketcap_current_ohlc,
                                                            cache.marketcap_latest_timestamp)

                elif AGGREGATED_TRADE_WS in ws_trade['stream']:
                    COUNTER += 1
                    aggtrade_data, symbol_pair = ws_trade['data'], ws_trade['data']['s']
                    coin_moment_price, coin_moment_trade_quantity = aggtrade_data['p'], aggtrade_data['q']

                    if symbol_pair in SP500_SYMBOLS_USDT_PAIRS:
                        coin_symbol = dts.remove_usdt(symbol_pair)
                        cache.coins_moment_price = dts.update_cached_coins_values(
                            cache.coins_moment_price, coin_symbol, coin_moment_price)

                        cache.marketcap_coins_value = dts.update_cached_marketcap_coins_value(
                            cache.marketcap_coins_value, coin_symbol, coin_moment_price, coin_ratio[coin_symbol])

                        cache.marketcap_sum = sum(list(cache.marketcap_coins_value.values()))

                        cache.coins_volume = dts.update_cached_coin_volumes(
                            cache.coins_volume, coin_symbol, coin_moment_trade_quantity)

                    cache.aggtrade_data = {symbol_pair: dts.clean_data(aggtrade_data, 'E', 'p', 'q')}

                    if COUNTER > AGGTRADE_PYCACHE:
                        await dts.insert_aggtrade_data(ta_lines_db, cache.aggtrade_data)

                        cache.aggtrade_data = {}

                        COUNTER -= AGGTRADE_PYCACHE


            except ServerSelectionTimeoutError as e:
                if "localhost:27017" in e.args[0]:
                    logging.exception("Cannot connect to mongo DB")
                    raise
                else:
                    logging.exception("Unexpected error")
                    raise
            except Exception as e:
                if ws_trade['m'] == 'Queue overflow. Message not filled':
                    raise QueueOverflow

                traceback.print_exc()
                print(f"{e}, {ws_trade}")

                exit(1)


async def main():
    candlestick_db = mongo.connect_to_usdt_candlestick_db()
    ta_lines_db = mongo.connect_to_TA_lines_db()

    bm = BinanceSocketManager(await AsyncClient.create())

    while True:
        try:
            await binance_to_mongodb(
                bm.multiplex_socket(dts.usdt_symbols_stream(CANDLESTICKS_ONE_MINUTE_WS) +
                                    dts.usdt_symbols_stream(AGGREGATED_TRADE_WS)),
                candlestick_db,
                ta_lines_db,
                dts.get_coin_fund_ratio(dts.remove_usdt(SP500_SYMBOLS_USDT_PAIRS),
                                        requests.get(coingecko_marketcap_api_link).json()))
        except QueueOverflow as e:
            pass
        except Exception as e:
            exit(1)


# TODO: clean symbols that start with usdt and not finish with them, acho que é um erro do binance... mas a variavel das moedas
#  tem 39 simbolos e os dicts 38, verificar qual falta.

# TODO: criar candles do marketcap para fazer o seu ATR, já tenho o valor sempre atual, de 2 em 2 segundos meter na candle.
# TODO: when implementing ATR, create abstraction for 1m to 5m candles, DB needs all 1m candles, ATR will be mostly used with 5m candles.
# TODO: create indexs
# TODO: implement coingecko refresh 24h.
# TODO: implement matplotlib to see TA.
# TODO: 14 candlestick value cached in python for ATR calc
#    database.createIndex(background: True)
# TODO: treat restart when QUEUE reaches limit.

# TODO: preciso: - rácio de cada simbolo últimos 15 minutos: cached_marketcap_candles
#                - rácio do fundo últimos 15 minutos:
#                - ATR dos simbolos todos, OHLC nos últimos 15m: cached_marketcap_candles
#                - ATR do fundo, OHLC nos últimos 15m:

# Todo: falta o trigger que vai de 5 em 5 segundos inserir na BD o volume (falta fazer este), o RS ( acho
# que já está tudo calculado ), e o preço atual.

# Todo: tenho de fazer as candles de 5m do ATR e do volume, o ratio pode ser atualizado sem fazer reset ao fund
# para "1.00",pois só mudam os ratios, os preços que subiram ou desceram continuam a fazer o valor ficar
# consistente, atualizar o ratio várias vezes para ser desprezievel a mudança no ratio.


if __name__ == "__main__":
    asyncio.run(main())

# async def insert_ta_data(ta_lines_db, symbol, candle_data, present_timestamp):
#     candle_data['t'] = present_timestamp
#     candle_dict = {symbol: candle_data}
#     await mongo.insert_in_db(ta_lines_db, candle_dict)
#     print(f"inserted {list(candle_dict.keys())[0]}")


# async def insert_update_data(candlestick_db, ta_lines_db, candles, data, present_timestamp):
#     current_symbol = data['s']
#     cleaned_data = clean_kline_data(data)
#
#     update_candles = await insert_update_kline_data(candlestick_db, candles, current_symbol, cleaned_data)
#
#     return update_candles
