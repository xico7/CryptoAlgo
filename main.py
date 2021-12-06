import data_staging as dts
from binance import AsyncClient, BinanceSocketManager
import MongoDB.DBactions as mongo
import asyncio
import logging
import time

# from binance.client import Client

CANDLESTICK_WS = "kline"
CANDLESTICKS_ONE_MINUTE_WS = "@kline_1m"
AGGREGATED_TRADE_WS = "@aggTrade"


async def insert_update_kline_data(db, candles, current_data):
    clean_kline_data = {current_data['s']: dts.clean_data(current_data, 't', 'v', 'o', 'h', 'l', 'c')}
    updated_candles, symbol_data = dts.data_feed(candles, clean_kline_data)

    if symbol_data:
        mongdb_symbol_data = dict(symbol_data)  # mongo async lib adds items to dict.
        await mongo.insert_in_db(db, mongdb_symbol_data)
        return updated_candles, symbol_data

    return updated_candles, None


async def insert_aggtrade_data(db, current_data):
    clean_aggtrade_data = {current_data['s']: dts.clean_data(current_data, 'E', 'p', 'q')}
    await mongo.insert_in_db(db, clean_aggtrade_data)


async def binance_to_mongodb(multisocket_candle, candlestick_db, ta_lines_db):
    atr_14_period_klines = {}
    current_klines = {}
    async with multisocket_candle as tscm:
        while True:
            ws_trade = await tscm.recv()
            try:
                if CANDLESTICK_WS in ws_trade['stream']:
                    kline_data = ws_trade['data']['k']
                    current_klines, new_symbol_kline = await insert_update_kline_data(candlestick_db,
                                                                                      current_klines,
                                                                                      kline_data)
                    if new_symbol_kline:
                        #atr_14_period_klines[list(new_symbol_kline.keys())[0]]['3'] = list(new_symbol_kline.values())
                        atr_14_period_klines.update(new_symbol_kline)

                elif AGGREGATED_TRADE_WS in ws_trade['stream']:
                    aggtrade_data = ws_trade['data']
                    await insert_aggtrade_data(ta_lines_db, aggtrade_data)
            except Exception as e:
                #TODO: treat restart when QUEUE reaches limit, lost trades are not really many, a few milliseconds for each minute.
                print(f"{e}, {ws_trade}")

            print(f"{time.time()} , {ws_trade['data']['E']} , calc")


async def main():
    candlestick_db = mongo.connect_to_usdt_candlestick_db()
    ta_lines_db = mongo.connect_to_TA_lines_db()

    binance_client = await AsyncClient.create()
    bm = BinanceSocketManager(binance_client)

    while True:
        await binance_to_mongodb(bm.multiplex_socket(
            dts.symbols_stream(CANDLESTICKS_ONE_MINUTE_WS) +
            dts.symbols_stream(AGGREGATED_TRADE_WS)),
            candlestick_db, ta_lines_db)
        pass


# TODO: create indexs
# TODO: implement RS from top 30 coingecko coins marketcap ratio each day.
# TODO: implement matplotlib to see TA.
# TODO: 14 candlestick value cached in python for ATR calc
#    database.createIndex(background: True)


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
