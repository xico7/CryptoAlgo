from datetime import time

import requests as requests

from binance import AsyncClient, BinanceSocketManager
import MongoDB.DBactions as mongo
import asyncio
import time

# from binance.client import Client
# def binance_credentials():
#     api_key = "7tmh1WvBaWmB2giUU3xCRbEJcQq19D9xrGz6yEwMllfTRNyE9IVW2Derk2rVyfV3"
#     api_secret = "4rWXYlGUkNs4FWRZlwJD9EQR6SlFXFkYYQAMY0gpOWH5hoGisRafob7PU9VJp99Z"
#     return Client(api_key, api_secret)

CANDLESTICKS_ONE_MINUTE = "@kline_1m"

# TODO: take out unwanted USDT pairs... usdc etc etc,.
def get_usdt_symbols() -> list:
    usdt_symbols = []
    binance_symbols_price = requests.get("https://api.binance.com/api/v3/ticker/price").json()

    for elem in binance_symbols_price:
        if "USDT" in elem['symbol']:
            usdt_symbols.append(elem['symbol'])

    return [element.lower() for element in usdt_symbols]


def symbols_stream(symbols: list, type_of_trade: str) -> list:
    return [f"{symbol}{type_of_trade}" for symbol in symbols]


# TODO: refactor insert_data, i don't like it.
def get_updated_data(latest_candles: list, current_trade: dict):
    timestamp = 't'
    close = 'c'
    high = 'h'
    low = 'l'
    volume = 'v'

    insert_data = None
    current_symbol = current_trade['s']

    # Prune unnecessary data, add new symbol to dict..
    if current_symbol not in latest_candles:
        for elem in ['T', 's', 'i', 'f', 'L', 'n', 'x', 'q', 'V', 'Q', 'B']:
            del current_trade[elem]

        latest_candles.update({current_symbol: current_trade})
        return latest_candles, insert_data

    # Candle timeframe changed, time to write to candle value to DB and reset symbol value.
    if current_trade[timestamp] > latest_candles[current_symbol][timestamp]:
        insert_data = {current_symbol: latest_candles[current_symbol]}
        del latest_candles[current_symbol]

        return latest_candles, insert_data

    # Close is always the newer value.
    latest_candles[current_symbol][close] = current_trade[close]
    # Volume always goes up in the same kline.
    latest_candles[current_symbol][volume] = current_trade[volume]

    # Update max if new max.
    if current_trade[high] > latest_candles[current_symbol][high]:
        latest_candles[current_symbol][high] = current_trade[high]
    # Update low if new low
    elif current_trade[low] < latest_candles[current_symbol][low]:
        latest_candles[current_symbol][low] = current_trade[low]

    return latest_candles, insert_data


async def binance_to_mongodb(ms, db):
    current_candles = {}
    async with ms as tscm:
        while True:
            ws_trade = await tscm.recv()
            candle_data = ws_trade['data']['k']

            updated_candles, symbol_to_insert = get_updated_data(current_candles, candle_data)

            if symbol_to_insert:
                await mongo.insert_in_db(db, symbol_to_insert)

            current_candles = updated_candles


async def main():
    db, client2 = mongo.connect_to_db()

    binance_client = await AsyncClient.create()
    bm = BinanceSocketManager(binance_client)

    while True:
        await binance_to_mongodb(bm.multiplex_socket(symbols_stream(get_usdt_symbols(), CANDLESTICKS_ONE_MINUTE)), db)
        print("done2")


# TODO: if new coin is present, create index and collection, verify this behavior and execute in background.
# TODO: implement threading for other funcionts.. index etc.

#    database.createIndex(background: True)


if __name__ == "__main__":
    asyncio.run(main())
