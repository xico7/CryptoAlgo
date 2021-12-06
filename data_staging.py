from typing import Tuple, Optional

import requests as requests


def clean_data(data, *args):
    data_keys = {}
    for arg in args:
        data_keys.update({arg: data[arg]})

    return data_keys


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