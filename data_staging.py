import re
from typing import Tuple, Optional, Dict, Any, Union, List
from main import ATR_INDEX_SIZE
import requests as requests
import MongoDB.DBactions as mongo


def remove_usdt(symbols: Union[list, str]) -> Union[str, List[str]]:
    if isinstance(symbols, str):
        return re.match('(^(.+?)USDT)', symbols).groups()[1].upper()
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
def get_usdt_symbols() -> list:
    usdt_symbols = []
    binance_symbols_price = requests.get("https://api.binance.com/api/v3/ticker/price").json()

    for elem in binance_symbols_price:
        if "USDT" in elem['symbol']:
            usdt_symbols.append(elem['symbol'])

    return [element.lower() for element in usdt_symbols]


def symbols_stream(type_of_trade: str, symbols=None) -> list:
    if not symbols:
        symbols = get_usdt_symbols()
    return [f"{symbol}{type_of_trade}" for symbol in symbols]


def data_feed(latest_candles: list, current_trade: dict) -> Tuple[list, Optional[dict]]:
    candle_open_timestamp = 't'
    close = 'c'
    high = 'h'
    low = 'l'
    volume = 'v'

    symbol = list(current_trade.keys())[0]

    # Prune unnecessary data, add new symbol to dict..
    if symbol not in latest_candles:
        latest_candles.update(current_trade)
        return latest_candles, None

    # Candle timeframe changed, time to write candle value into DB and reset symbol value.
    if current_trade[symbol][candle_open_timestamp] > latest_candles[symbol][candle_open_timestamp]:
        insert_data = {symbol: latest_candles[symbol]}
        del latest_candles[symbol]

        return latest_candles, insert_data

    # Close is always the newer value.
    latest_candles[symbol][close] = current_trade[symbol][close]
    # Volume always goes up in the same kline.
    latest_candles[symbol][volume] = current_trade[symbol][volume]

    # Update max if new max.
    if current_trade[symbol][high] > latest_candles[symbol][high]:
        latest_candles[symbol][high] = current_trade[symbol][high]
    # Update low if new low
    elif current_trade[symbol][low] < latest_candles[symbol][low]:
        latest_candles[symbol][low] = current_trade[symbol][low]

    return latest_candles, None


def update_atr(kline_data):
    atr = {}
    symbol = list(kline_data.keys())[0]

    if symbol not in atr:
        atr.update({symbol: {1: list(kline_data.values())}})
    else:
        atr_last_index = max(list(atr[symbol]))

        if atr_last_index < ATR_INDEX_SIZE:
            atr[symbol][atr_last_index + 1] = list(kline_data.values())
        else:
            for elem in atr[symbol]:
                if not elem == atr_last_index:
                    atr[symbol][elem] = atr[symbol][elem + 1]
                else:
                    atr[symbol][atr_last_index] = list(kline_data.values())
    return atr


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
