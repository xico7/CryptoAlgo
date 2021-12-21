import traceback

import requests
import re
import data_staging as dts
from binance import AsyncClient, BinanceSocketManager
import MongoDB.DBactions as mongo
import asyncio
import logging
import time

logging.basicConfig(level=logging.DEBUG)

sp500_symbols_usdt_pairs = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'LUNAUSDT',
                            'DOGEUSDT',
                            'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'TRXUSDT', 'BCHUSDT',
                            'ALGOUSDT',
                            'MANAUSDT', 'XLMUSDT', 'AXSUSDT', 'VETUSDT', 'FTTUSDT', 'EGLDUSDT', 'ATOMUSDT', 'ICPUSDT',
                            'FILUSDT',
                            'HBARUSDT', 'SANDUSDT', 'THETAUSDT', 'FTMUSDT',
                            'NEARUSDT', 'BTTUSDTXTZUSDT', 'XMRUSDT', 'KLAYUSDT', 'GALAUSDT', 'HNTUSDT', 'GRTUSDT',
                            'LRCUSDT']

cached_coins_volume = {}
cached_coin_present_values = {}
cached_marketcap_coins_value = {}
cached_marketcap_sum = 0

cached_marketcap_latest_timestamp = 0
cached_current_marketcap_ohlc = {'t': 0, 'h': 0, 'o': 0, 'l': 999999999999999, 'c': 0}
cached_current_ohlcs = {}

cached_marketcap_ohlc_data = {}
cached_symbols_ohlc_data = {}

coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/" \
                               "markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"

CANDLESTICK_WS = "kline"
CANDLESTICKS_ONE_MINUTE_WS = "@kline_1m"
AGGREGATED_TRADE_WS = "@aggTrade"
OHLC_CACHE_PERIODS = 3


async def binance_to_mongodb(multisocket_candle, candlestick_db, ta_lines_db, coin_ratio):
    time_counter = int(time.time())

    # TODO: preciso: - rácio de cada simbolo últimos 15 minutos: cached_marketcap_candles
    #                - rácio do fundo últimos 15 minutos:
    #                - ATR dos simbolos todos, OHLC nos últimos 15m: cached_marketcap_candles
    #                - ATR do fundo, OHLC nos últimos 15m:

    # Todo: falta o trigger que vai de 5 em 5 segundos inserir na BD o volume (falta fazer este), o RS ( acho
    # que já está tudo calculado ), e o preço atual.

    # Todo: tenho de fazer as candles de 5m do ATR e do volume, o ratio pode ser atualizado sem fazer reset ao fund
    # para "1.00",pois só mudam os ratios, os preços que subiram ou desceram continuam a fazer o valor ficar
    # consistente, atualizar o ratio várias vezes para ser desprezievel a mudança no ratio.

    async with multisocket_candle as tscm:
        while True:
            try:
                global cached_current_ohlcs, cached_symbols_ohlc_data, cached_marketcap_ohlc_data, cached_marketcap_latest_timestamp



                ws_trade = await tscm.recv()
                if int(time.time()) > time_counter + 2 and cached_marketcap_latest_timestamp > 0:
                    time_counter += 2
                    dts.update_current_marketcap_ohlc_data(cached_current_marketcap_ohlc,
                                                           cached_marketcap_latest_timestamp,
                                                           cached_marketcap_sum)

                if CANDLESTICK_WS in ws_trade['stream']:

                    cached_current_ohlcs, cached_symbols_ohlc_data, cached_marketcap_ohlc_data, cached_marketcap_latest_timestamp = \
                        await dts.transform_candles(cached_current_ohlcs,
                                                    ws_trade['data']['k'],
                                                    candlestick_db,
                                                    cached_symbols_ohlc_data,
                                                    cached_marketcap_ohlc_data,
                                                    cached_current_marketcap_ohlc,
                                                    cached_marketcap_latest_timestamp,
                                                    OHLC_CACHE_PERIODS)


                elif AGGREGATED_TRADE_WS in ws_trade['stream']:
                    aggtrade_data = ws_trade['data']

                    symbol_pair = ws_trade['data']['s']

                    aggtrade_clean_data = dts.clean_data(aggtrade_data, 'E', 'p', 'q')

                    if symbol_pair in sp500_symbols_usdt_pairs:
                        coin_symbol = dts.remove_usdt(symbol_pair)
                        cached_coin_present_values.update({coin_symbol: float(aggtrade_data['p'])})

                        cached_marketcap_coins_value.update({coin_symbol: (float(aggtrade_data['p']) *
                                                                           coin_ratio[coin_symbol])})
                        cached_marketcap_sum = sum(list(cached_marketcap_coins_value.values()))

                        if coin_symbol in cached_coins_volume:
                            cached_coins_volume[coin_symbol] += float(aggtrade_data['q'])
                        else:
                            cached_coins_volume.update({coin_symbol: float(aggtrade_data['q'])})

                    await dts.insert_aggtrade_data(ta_lines_db, symbol_pair, aggtrade_clean_data)

                print(f"{time.time()} , {ws_trade['data']['E']} , calc")

            except Exception as e:
                traceback.print_exc()
                print(f"{e}, {ws_trade}")
                exit(1)


async def main():
    candlestick_db = mongo.connect_to_usdt_candlestick_db()
    ta_lines_db = mongo.connect_to_TA_lines_db()
    coin_normalized_ratio = dts.sp500_multiply_usdt_ratio(dts.remove_usdt(sp500_symbols_usdt_pairs),
                                                          coingecko_marketcap_api_link)

    binance_client = await AsyncClient.create()
    bm = BinanceSocketManager(binance_client)

    while True:
        await binance_to_mongodb(bm.multiplex_socket(
            dts.usdt_symbols_stream(CANDLESTICKS_ONE_MINUTE_WS) +
            dts.usdt_symbols_stream(AGGREGATED_TRADE_WS)),
            candlestick_db, ta_lines_db, coin_normalized_ratio)

        print("ABC")


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

# TODO: checklist: Volume -> DONE, ATR


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
