import re
from typing import Tuple, Optional, Dict, Any, Union, List
import requests as requests
import MongoDB.DBactions as mongo

# from main import cached_marketcap_ohlc_data, cached_current_marketcap_candle, cached_symbols_ohlc_data, CANDLE_CACHE_PERIODS

TIMESTAMP = 't'
OPEN = 'o'
CLOSE = 'c'
HIGH = 'h'
LOW = 'l'
VOLUME = 'v'


def remove_usdt(symbols: Union[list, str]) -> Union[str, List[str]]:
    if isinstance(symbols, str):
        try:
            return re.match('(^(.+?)USDT)', symbols).groups()[1].upper()
        except AttributeError as AttrError:
            print(symbols, AttrError)
            pass
    else:
        return [re.match('(^(.+?)USDT)', symbol).groups()[1].upper() for symbol in symbols]


def clean_data(data, *args):
    data_keys = {}
    for arg in args:
        data_keys.update({arg: data[arg]})

    return data_keys


async def insert_aggtrade_data(db, data_symbol, data):
    aggtrade_symbol_data = {data_symbol: data}
    await mongo.insert_in_db(db, aggtrade_symbol_data)


# TODO: take out unwanted USDT pairs... usdc etc etc,.
def query_usdt_symbols() -> list:
    usdt_symbols = []
    binance_symbols_price = requests.get("https://api.binance.com/api/v3/ticker/price").json()

    for elem in binance_symbols_price:
        if "USDT" in elem['symbol']:
            usdt_symbols.append(elem['symbol'])

    return [element.lower() for element in usdt_symbols]


def usdt_symbols_stream(type_of_trade: str, symbols=None) -> list:
    if not symbols:
        symbols = query_usdt_symbols()
    return [f"{symbol}{type_of_trade}" for symbol in symbols]


async def transform_candles(cached_current_ohlcs: dict,
                            trade_data: dict,
                            candlestick_db,
                            cached_symbols_ohlc_data: dict,
                            cached_marketcap_ohlc_data: dict,
                            cached_current_marketcap_ohlc: dict,
                            cached_marketcap_latest_timestamp: int,
                            ohlc_periods: int):

    cached_current_ohlcs_copy = cached_current_ohlcs.copy()
    trade_data_copy = trade_data.copy()
    cached_symbols_ohlc_data_copy = cached_symbols_ohlc_data.copy()
    cached_marketcap_ohlc_data_copy = cached_marketcap_ohlc_data.copy()
    cached_current_marketcap_candle_copy = cached_current_marketcap_ohlc.copy()

    ohlc_trade_data = {trade_data_copy['s']: clean_data(trade_data_copy, 't', 'v', 'o', 'h', 'l', 'c')}

    symbol_pair = list(ohlc_trade_data.keys())[0]

    if symbol_pair not in cached_current_ohlcs_copy:
        cached_current_ohlcs_copy.update(ohlc_trade_data)

    cached_current_ohlcs_copy[symbol_pair] = update_current_symbol_ohlc(cached_current_ohlcs_copy[symbol_pair],
                                                                        ohlc_trade_data[symbol_pair])

    # Candle timeframe changed, time to write candle value into DB and reset symbol value.
    if ohlc_trade_data[symbol_pair][TIMESTAMP] > cached_current_ohlcs_copy[symbol_pair][TIMESTAMP]:
        new_ohlc_data = {symbol_pair: cached_current_ohlcs_copy[symbol_pair]}

        await mongo.insert_in_db(candlestick_db, new_ohlc_data)
        del cached_current_ohlcs_copy[symbol_pair]
        cached_symbols_ohlc_data_copy = update_cached_symbols_ohlc_data(cached_symbols_ohlc_data_copy,
                                                                        new_ohlc_data,
                                                                        ohlc_periods)

        if ohlc_trade_data[symbol_pair][TIMESTAMP] > cached_marketcap_latest_timestamp:
            cached_marketcap_latest_timestamp = ohlc_trade_data[symbol_pair][TIMESTAMP]
            if cached_marketcap_latest_timestamp > 0:
                cached_marketcap_ohlc_data_copy = update_cached_marketcap_ohlc_data(cached_marketcap_ohlc_data_copy,
                                                                                    cached_current_marketcap_candle_copy,
                                                                                    ohlc_periods)

            # dts.insert_mktcap_candle_db()
            # reset mktcap_candle

    return cached_current_ohlcs_copy, cached_symbols_ohlc_data_copy, cached_marketcap_ohlc_data_copy, cached_marketcap_latest_timestamp


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
    symbol = list(new_ohlc_data.keys())[0]

    if symbol not in ohlc_data:
        ohlc_data.update({symbol: {1: list(new_ohlc_data.values())[0]}})
    else:
        atr_last_index = max(list(ohlc_data[symbol]))

        if atr_last_index < cache_periods:
            ohlc_data[symbol][atr_last_index + 1] = list(new_ohlc_data.values())
        else:
            for elem in ohlc_data[symbol]:
                if not elem == atr_last_index:
                    ohlc_data[symbol][elem] = ohlc_data[symbol][elem + 1]
                else:
                    ohlc_data[symbol][atr_last_index] = list(new_ohlc_data.values())
    return ohlc_data


def update_cached_marketcap_ohlc_data(cached_marketcap_ohlc_data_copy: dict, cached_current_marketcap_candle: dict,
                                      candle_periods: int) -> dict:
    if not cached_marketcap_ohlc_data_copy:
        cached_marketcap_ohlc_data_copy.update({1: cached_current_marketcap_candle})
        return cached_marketcap_ohlc_data_copy

    last_index = max(list(cached_marketcap_ohlc_data_copy))

    if last_index < candle_periods:
        cached_marketcap_ohlc_data_copy.update({last_index + 1: cached_current_marketcap_candle})
    else:
        for elem in cached_marketcap_ohlc_data_copy:
            if not elem == last_index:
                cached_marketcap_ohlc_data_copy[elem] = cached_marketcap_ohlc_data_copy[elem + 1]
            else:
                cached_marketcap_ohlc_data_copy.update({candle_periods: cached_current_marketcap_candle})

    return cached_marketcap_ohlc_data_copy


def update_current_marketcap_ohlc_data(marketcap_ohlc: dict, timestamp: int, marketcap_moment_value: dict) -> dict:

    marketcap_ohlc_copy = marketcap_ohlc.copy()
    marketcap_moment_value_copy = marketcap_moment_value.copy()

    marketcap_ohlc_copy[TIMESTAMP] = timestamp
    if marketcap_ohlc_copy[OPEN] == 0:
        marketcap_ohlc_copy[OPEN] = marketcap_moment_value_copy

    marketcap_ohlc_copy[CLOSE] = marketcap_moment_value_copy
    if marketcap_moment_value_copy > marketcap_ohlc_copy[HIGH]:
        marketcap_ohlc_copy[HIGH] = marketcap_moment_value_copy
    if marketcap_moment_value_copy < marketcap_ohlc_copy[LOW]:
        marketcap_ohlc_copy[LOW] = marketcap_moment_value_copy

    return marketcap_ohlc_copy


def sp500_multiply_usdt_ratio(symbol_pairs: dict, api: str) -> Dict[Any, Union[float, Any]]:
    symbols_information = requests.get(api).json()

    sp500_symbols = {}

    for idx, symbol_info in enumerate(symbols_information):
        current_symbol = symbol_info['symbol'].upper()  # normalize symbols to uppercase.
        if current_symbol in symbol_pairs:
            sp500_symbols.update(
                {current_symbol: {'price': symbol_info['current_price'],
                                  'market_cap': symbol_info['market_cap']}})

    sp500_marketcap = sum([sp500_symbols[elem]['market_cap'] for elem in sp500_symbols])

    mulitply_coin_ratio = {}
    for elem in sp500_symbols:
        mulitply_coin_ratio.update({elem: sp500_symbols[elem]['market_cap'] / sp500_symbols[elem]['price']})

    return mulitply_coin_ratio

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
