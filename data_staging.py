import copy
import re
import time
from typing import Optional, Union, List
import requests as requests
import MongoDB.DBactions as mongo
import numpy as np
import talib
from numpy import double

from MongoDB.DB_OHLC_Create import TIME, RELATIVE_STRENGTH, PRICE
from main import OHLC_CACHE_PERIODS, REL_STRENGTH_PERIODS, RS_CACHE

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
            return None
    else:
        return [re.match('(^(.+?)USDT)', symbol).groups()[1].upper() for symbol in symbols]


def clean_data(data, *args):
    data_keys = {}
    for arg in args:
        data_keys.update({arg: data[arg]})

    return data_keys


async def insert_aggtrade_data(db, aggtrade_data):
    await mongo.duplicate_insert_aggtrade_data(db, aggtrade_data)


# async def insert_rs_volume_price(db, rel_strength):
#     await mongo.duplicate_insert_data_rs_volume_price(db, rel_strength)


def usdt_symbols_stream(type_of_trade: str) -> list:
    binance_symbols_price = requests.get("https://api.binance.com/api/v3/ticker/price").json()
    symbols = []

    for symbol_info in binance_symbols_price:
        if "USDT" in symbol_info[SYMBOL]:
            symbols.append(symbol_info[SYMBOL])
    return [f"{symbol.lower()}{type_of_trade}" for symbol in symbols]


async def update_ohlc_cached_values(current_ohlcs: dict, ws_trade_data: dict, symbols_ohlc_data: dict,
                                    marketcap_ohlc_data: dict, marketcap_current_ohlc: dict, marketcap_latest_timestamp: int):

    ohlc_trade_data = {ws_trade_data['s']: clean_data(ws_trade_data, TIMESTAMP, VOLUME, OPEN, HIGH, LOW, CLOSE)}
    symbol_pair = list(ohlc_trade_data.keys())[0]

    if symbol_pair not in current_ohlcs:
        current_ohlcs.update(ohlc_trade_data)

    current_ohlcs[symbol_pair] = update_current_symbol_ohlc(current_ohlcs[symbol_pair], ohlc_trade_data[symbol_pair])

    # Candle timeframe changed, time to write candle value into DB and reset symbol value.
    if ohlc_trade_data[symbol_pair][TIMESTAMP] > current_ohlcs[symbol_pair][TIMESTAMP]:
        new_ohlc_data = {symbol_pair: current_ohlcs[symbol_pair]}

        del current_ohlcs[symbol_pair]
        symbols_ohlc_data = update_cached_symbols_ohlc_data(symbols_ohlc_data, new_ohlc_data, OHLC_CACHE_PERIODS)

        if ohlc_trade_data[symbol_pair][TIMESTAMP] > marketcap_latest_timestamp:
            marketcap_latest_timestamp = ohlc_trade_data[symbol_pair][TIMESTAMP]  # Update marketcap latest timestamp
            if marketcap_current_ohlc[TIMESTAMP] > 0 and not marketcap_ohlc_data:
                marketcap_ohlc_data = copy.deepcopy(update_cached_marketcap_ohlc_data(marketcap_ohlc_data, marketcap_current_ohlc))
            if marketcap_ohlc_data and (marketcap_ohlc_data[len(marketcap_ohlc_data)][TIMESTAMP] != marketcap_current_ohlc[TIMESTAMP]):
                marketcap_ohlc_data = copy.deepcopy(update_cached_marketcap_ohlc_data(marketcap_ohlc_data, marketcap_current_ohlc))

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


def update_cached_symbols_ohlc_data(ohlc_data: dict, new_ohlc_data: dict, cache_periods: int) -> Optional[dict]:

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


def update_current_marketcap_ohlc_data(marketcap_ohlc: dict, timestamp: int, marketcap_moment_value: float) -> dict:
    if marketcap_ohlc[TIMESTAMP] != timestamp:
        marketcap_ohlc[TIMESTAMP] = timestamp
        marketcap_ohlc[OPEN] = 0
        marketcap_ohlc[HIGH] = marketcap_ohlc[CLOSE] = marketcap_ohlc[LOW] = marketcap_moment_value
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
    cached_coins_values.update({coin_symbol: coin_moment_price})

    return cached_coins_values


def update_cached_coin_volumes(cached_coins_volume: dict, coin_symbol: str, coin_moment_trade_quantity: float) -> dict:
    if coin_symbol in cached_coins_volume:
        cached_coins_volume[coin_symbol] += coin_moment_trade_quantity
    else:
        cached_coins_volume.update({coin_symbol: coin_moment_trade_quantity})

    return cached_coins_volume


def update_cached_marketcap_coins_value(cached_marketcap_coins_value: dict,
                                        coin_symbol: str,
                                        coin_moment_price: float,
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


# TODO: debug this function for cases where it reaches unconclusive values, if it happens often it may be a problem.
def calculate_relative_strength(coin_ohlc_data, marketcap_rel_atr, cached_marketcap_ohlc_data) -> Optional[float]:
    coin_relative_atr = calculate_relative_atr(coin_ohlc_data)
    coin_change_percentage = ((float(coin_ohlc_data[len(coin_ohlc_data)][OPEN]) / float(coin_ohlc_data[1][OPEN])) - 1) * 100

    try:
        market_change_percentage = ((cached_marketcap_ohlc_data[len(coin_ohlc_data)][OPEN] / cached_marketcap_ohlc_data[1][OPEN]) - 1) * 100
    except ZeroDivisionError:
        return None  # unlikely case, no better solution found.

    atr_quotient = (coin_relative_atr / marketcap_rel_atr)

    if atr_quotient > 15 or atr_quotient < -15 or atr_quotient == 0 or (0.00001 > atr_quotient > 0):
        return None

    return (coin_change_percentage - market_change_percentage) / atr_quotient


def update_relative_strength_cache(marketcap_ohlc_data, coins_ohlc_data,
                                   coins_volume, coins_moment_price,
                                   coins_rel_strength, rs_cache_counter,
                                   rel_strength_db):


    marketcap_relative_atr = calculate_relative_atr(marketcap_ohlc_data)
    for coin_ohlc_data in coins_ohlc_data.items():
        if len(coin_ohlc_data[1]) == OHLC_CACHE_PERIODS:
            try:
                get_coin_volume = coins_volume[remove_usdt(coin_ohlc_data[0])]
                get_coin_moment_price = coins_moment_price[remove_usdt(coin_ohlc_data[0])]
            except KeyError:
                continue

            relative_strength = calculate_relative_strength(
                coin_ohlc_data[1], marketcap_relative_atr,
                marketcap_ohlc_data)
            if relative_strength:
                coins_rel_strength = {coin_ohlc_data[0]:
                                                {TIME: get_current_time(),
                                                 RELATIVE_STRENGTH: relative_strength,
                                                 VOLUME: get_coin_volume,
                                                 PRICE: get_coin_moment_price}
                                            }

            rs_cache_counter += 1

            return coins_rel_strength, rs_cache_counter

def get_current_time() -> int:
    return int(time.time())



    # marketcap_relative_atr = dts.calculate_relative_atr(cache.marketcap_ohlc_data)
    # for coin_ohlc_data in cache.coins_ohlc_data.items():
    #     if len(coin_ohlc_data[1]) == OHLC_CACHE_PERIODS:
    #         try:
    #             get_coin_volume = cache.coins_volume[dts.remove_usdt(coin_ohlc_data[0])]
    #             get_coin_moment_price = cache.coins_moment_price[dts.remove_usdt(coin_ohlc_data[0])]
    #         except KeyError:
    #             continue
    #
    #         relative_strength = dts.calculate_relative_strength(
    #             coin_ohlc_data[1], marketcap_relative_atr,
    #             cache.marketcap_ohlc_data)
    #         if relative_strength:
    #             cache.coins_rel_strength = {coin_ohlc_data[0]:
    #                                             {TIME: get_current_time(),
    #                                              RELATIVE_STRENGTH: relative_strength,
    #                                              VOLUME: get_coin_volume,
    #                                              PRICE: get_coin_moment_price}
    #                                         }
    #
    #         rs_cache_counter += 1
    #         cur_time = get_current_time()
    #         new_minute_ohlc = cur_time % 60 == 0 or (cur_time + 1) % 60 == 0 or (
    #                 cur_time - 1) % 60 == 0
    #
    #         if rs_cache_counter > RS_CACHE or new_minute_ohlc:
    #             await mongo.duplicate_insert_data_rs_volume_price(rel_strength_db, cache.coins_rel_strength)
    #             # await dts.insert_rs_volume_price(rel_strength_db, cache.coins_rel_strength)
    #             cache.coins_rel_strength = {}
    #             rs_cache_counter = 0
    #             if new_minute_ohlc:
    #                 while (cur_time - 3) % 60 != 0:
    #                     cur_time -= 1
    #                 last_ohlc_open_timestamp = cur_time - 3
    #                 mongoDBcreate.insert_ohlc_data(last_ohlc_open_timestamp,
    #                                              ohlc_1m_db, ohlc_5m_db, ohlc_15m_db,
    #                                              ohlc_1h_db, ohlc_4h_db, ohlc_1d_db)
    #                 # t = Thread(target=mongoDBcreate.insert_ohlc_data, args=(last_ohlc_open_timestamp,
    #                 #                                                       ohlc_1m_db, ohlc_5m_db, ohlc_15m_db,
    #                 #                                                       ohlc_1h_db, ohlc_4h_db, ohlc_1d_db, ))
    #                 # t.start()