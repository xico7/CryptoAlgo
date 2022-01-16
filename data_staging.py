import re
from typing import Tuple, Optional, Dict, Any, Union, List
import requests as requests
import MongoDB.DBactions as mongo
import numpy as np
import talib
from numpy import double

# from main import cached_marketcap_ohlc_data, cached_current_marketcap_candle, cached_symbols_ohlc_data, CANDLE_CACHE_PERIODS
from main import OHLC_CACHE_PERIODS, REL_STRENGTH_PERIODS

# OHLC
TIMESTAMP = 't'
OPEN = 'o'
CLOSE = 'c'
HIGH = 'h'
LOW = 'l'
VOLUME = 'v'

SYMBOL = 'symbol'


def remove_usdt(symbols: Union[List[str], str]):
    if isinstance(symbols, str):
        try:
            return re.match('(^(.+?)USDT)', symbols).groups()[1].upper()
        except AttributeError as AttrError:
            # TODO: review this exception
            print(symbols, AttrError)
            pass
    else:
        return [re.match('(^(.+?)USDT)', symbol).groups()[1].upper() for symbol in symbols]


def clean_data(data, *args):
    data_keys = {}
    for arg in args:
        data_keys.update({arg: data[arg]})

    return data_keys


async def insert_aggtrade_data(db, data_symbol, aggtrade_data):
    await mongo.insert_in_db(db, {data_symbol: clean_data(aggtrade_data, 'E', 'p', 'q')})


def usdt_symbols_stream(type_of_trade: str) -> list:
    binance_symbols_price = requests.get("https://api.binance.com/api/v3/ticker/price").json()

    symbols = []
    for symbol_info in binance_symbols_price:
        if "USDT" in symbol_info[SYMBOL]:
            symbols.append(symbol_info[SYMBOL])
    return [f"{symbol.lower()}{type_of_trade}" for symbol in symbols]


async def update_ohlc_cached_values(current_ohlcs: dict, ws_trade_data: dict, mongodb, symbols_ohlc_data: dict,
                                    marketcap_ohlc_data: dict, current_marketcap_ohlc: dict, marketcap_latest_timestamp: int):

    ohlc_trade_data = {ws_trade_data['s']: clean_data(ws_trade_data, 't', 'v', 'o', 'h', 'l', 'c')}
    symbol_pair = list(ohlc_trade_data.keys())[0]

    if symbol_pair not in current_ohlcs:
        current_ohlcs.update(ohlc_trade_data)

    current_ohlcs[symbol_pair] = update_current_symbol_ohlc(current_ohlcs[symbol_pair],
                                                            ohlc_trade_data[symbol_pair])

    # Candle timeframe changed, time to write candle value into DB and reset symbol value.
    if ohlc_trade_data[symbol_pair][TIMESTAMP] > current_ohlcs[symbol_pair][TIMESTAMP]:
        new_ohlc_data = {symbol_pair: current_ohlcs[symbol_pair]}

        await mongo.insert_in_db(mongodb, new_ohlc_data)
        del current_ohlcs[symbol_pair]
        symbols_ohlc_data = update_cached_symbols_ohlc_data(symbols_ohlc_data,
                                                            new_ohlc_data,
                                                            OHLC_CACHE_PERIODS)

        if ohlc_trade_data[symbol_pair][TIMESTAMP] > marketcap_latest_timestamp:
            marketcap_latest_timestamp = ohlc_trade_data[symbol_pair][TIMESTAMP]  # Update marketcap latest timestamp
            if current_marketcap_ohlc[TIMESTAMP] > 0:
                marketcap_ohlc_data = update_cached_marketcap_ohlc_data(marketcap_ohlc_data, current_marketcap_ohlc)

    return current_ohlcs, symbols_ohlc_data, marketcap_ohlc_data, marketcap_latest_timestamp


def update_current_symbol_ohlc(current_symbol_ohlc, ohlc_trade_data):
    # Close is always the newest value.
    current_symbol_ohlc[CLOSE] = ohlc_trade_data[CLOSE]
    # Volume always goes up in the same kline.
    current_symbol_ohlc[VOLUME] = ohlc_trade_data[VOLUME]

    # Update max if new max.
    if ohlc_trade_data[HIGH] > current_symbol_ohlc[HIGH]:
        current_symbol_ohlc[HIGH] = ohlc_trade_data[HIGH]
    # Update low if new low
    elif ohlc_trade_data[LOW] < current_symbol_ohlc[LOW]:
        current_symbol_ohlc[LOW] = ohlc_trade_data[LOW]

    return current_symbol_ohlc


def update_cached_symbols_ohlc_data(ohlc_data: dict, new_ohlc_data: dict, cache_periods: int) -> dict:

    new_ohlc_symbol = list(new_ohlc_data.keys())[0]
    new_ohlc_values = list(new_ohlc_data.values())[0]

    if new_ohlc_symbol not in ohlc_data:
        ohlc_data.update({new_ohlc_symbol: {1: new_ohlc_values}})
    else:
        atr_last_index = max(list(ohlc_data[new_ohlc_symbol]))

        if atr_last_index < cache_periods:
            ohlc_data[new_ohlc_symbol][atr_last_index + 1] = new_ohlc_values
        else:
            for elem in ohlc_data[new_ohlc_symbol]:
                if not elem == atr_last_index:
                    ohlc_data[new_ohlc_symbol][elem] = ohlc_data[new_ohlc_symbol][elem + 1]
                else:
                    ohlc_data[new_ohlc_symbol][atr_last_index] = new_ohlc_values
    return ohlc_data


def update_cached_marketcap_ohlc_data(cached_marketcap_ohlc_data_copy: dict, cached_current_marketcap_candle: dict) -> dict:
    if not cached_marketcap_ohlc_data_copy:
        cached_marketcap_ohlc_data_copy.update({1: cached_current_marketcap_candle})
        return cached_marketcap_ohlc_data_copy

    last_index = max(list(cached_marketcap_ohlc_data_copy))

    if last_index < OHLC_CACHE_PERIODS:
        cached_marketcap_ohlc_data_copy.update({last_index + 1: cached_current_marketcap_candle})
    else:
        for elem in cached_marketcap_ohlc_data_copy:
            if not elem == last_index:
                cached_marketcap_ohlc_data_copy[elem] = cached_marketcap_ohlc_data_copy[elem + 1]
            else:
                cached_marketcap_ohlc_data_copy.update({OHLC_CACHE_PERIODS: cached_current_marketcap_candle})

    return cached_marketcap_ohlc_data_copy


def update_current_marketcap_ohlc_data(marketcap_ohlc: dict, timestamp: int, marketcap_moment_value: dict) -> dict:
    if marketcap_ohlc[TIMESTAMP] != timestamp:
        marketcap_ohlc[TIMESTAMP] = timestamp
        marketcap_ohlc[OPEN] = marketcap_moment_value
        marketcap_ohlc[HIGH] = marketcap_moment_value
        marketcap_ohlc[CLOSE] = marketcap_moment_value
        marketcap_ohlc[LOW] = marketcap_moment_value
    else:
        if marketcap_ohlc[OPEN] == 0:
            marketcap_ohlc[OPEN] = marketcap_moment_value

        marketcap_ohlc[CLOSE] = marketcap_moment_value
        if marketcap_moment_value > marketcap_ohlc[HIGH]:
            marketcap_ohlc[HIGH] = marketcap_moment_value
        if marketcap_moment_value < marketcap_ohlc[LOW]:
            marketcap_ohlc[LOW] = marketcap_moment_value

    return marketcap_ohlc


def update_cached_coins_values(cached_coins_values: dict, coin_symbol: str, coin_moment_price: float) -> dict:
    cached_coins_values.update({coin_symbol: float(coin_moment_price)})

    return cached_coins_values


def update_cached_coin_volumes(cached_coins_volume: dict, coin_symbol: str, coin_moment_price: float) -> dict:
    if coin_symbol in cached_coins_volume:
        cached_coins_volume[coin_symbol] += float(coin_moment_price)
    else:
        cached_coins_volume.update({coin_symbol: float(coin_moment_price)})

    return cached_coins_volume


def update_cached_marketcap_coins_value(cached_marketcap_coins_value: dict,
                                        coin_symbol: str, coin_moment_price: float,
                                        coin_ratio: float) -> dict:
    cached_marketcap_coins_value.update({coin_symbol: (float(coin_moment_price) * coin_ratio)})

    return cached_marketcap_coins_value


def get_coin_fund_ratio(symbol_pairs: dict, symbols_information: dict):
    coin_ratio = {}

    for symbol_info in symbols_information:
        current_symbol = symbol_info[SYMBOL].upper()
        if current_symbol in symbol_pairs:
            coin_ratio.update({current_symbol: symbol_info['market_cap'] / symbol_info['current_price']})

    return coin_ratio



def calculate_relative_atr(ohlc_data):
    high, low, close = [], [], []
    for item in ohlc_data.items():
        high.append(float(item[1][HIGH]))
        low.append(float(item[1][LOW]))
        close.append(float(item[1][CLOSE]))

    average_true_range = talib.ATR(np.array(high), np.array(low), np.array(close), timeperiod=REL_STRENGTH_PERIODS)[REL_STRENGTH_PERIODS]

    return double(average_true_range) / double(ohlc_data[REL_STRENGTH_PERIODS][CLOSE]) * 100


def calculate_relative_strength(coin_ohlc_data, marketcap_rel_atr, cached_marketcap_ohlc_data):
    coin_relative_atr = calculate_relative_atr(coin_ohlc_data)

    last_element = len(coin_ohlc_data)
    first_element = len(coin_ohlc_data) - REL_STRENGTH_PERIODS
    coin_change_percentage = (float(coin_ohlc_data[last_element][OPEN]) /
                              float(coin_ohlc_data[first_element][OPEN]) - 1) * 100
    market_change_percentage = (float(cached_marketcap_ohlc_data[last_element][OPEN]) /
                                float(cached_marketcap_ohlc_data[first_element][OPEN]) - 1) * 100

    return (coin_change_percentage - market_change_percentage) / (coin_relative_atr / marketcap_rel_atr)


    #marketcap_relative_atr


# def sp500_normalized_one_usdt_ratio(symbol_pairs: dict, api: str) -> Dict[Any, Union[float, Any]]:
#     symbols_information = requests.get(api).json()
#
#     sp500_symbols = {}
#
#     for idx, symbol_info in enumerate(symbols_information):
#         current_symbol = symbol_info['symbol'].upper()  # normalize symbols to uppercase.
#         if current_symbol in symbol_pairs:
#             sp500_symbols.update(
#                 {current_symbol: {'price': symbol_info['current_price'],
#                                   'market_cap': symbol_info['market_cap']}})
#
#     sp500_marketcap = sum([sp500_symbols[elem]['market_cap'] for elem in sp500_symbols])
#
#     normalized_coin_ratio = {}
#     for elem in sp500_symbols:
#         normalized_coin_ratio.update({elem: sp500_symbols[elem]['market_cap'] / sp500_marketcap / sp500_symbols[elem]['price']})
#
#     return normalized_coin_ratio
