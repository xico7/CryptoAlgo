import requests
import re
import data_staging as dts
from binance import AsyncClient, BinanceSocketManager
import MongoDB.DBactions as mongo
import asyncio
import logging
import time

sp500_symbols_usdt_pairs = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'ADAUSDT', 'XRPUSDT', 'DOTUSDT', 'LUNAUSDT',
                            'DOGEUSDT',
                            'AVAXUSDT', 'SHIBUSDT', 'MATICUSDT', 'LTCUSDT', 'UNIUSDT', 'LINKUSDT', 'TRXUSDT', 'BCHUSDT',
                            'ALGOUSDT',
                            'MANAUSDT', 'XLMUSDT', 'AXSUSDT', 'VETUSDT', 'FTTUSDT', 'EGLDUSDT', 'ATOMUSDT', 'ICPUSDT',
                            'FILUSDT',
                            'HBARUSDT', 'SANDUSDT', 'THETAUSDT', 'FTMUSDT',
                            'NEARUSDT', 'BTTUSDTXTZUSDT', 'XMRUSDT', 'KLAYUSDT', 'GALAUSDT', 'HNTUSDT', 'GRTUSDT',
                            'LRCUSDT']

coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/" \
                               "markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"

CANDLESTICK_WS = "kline"
CANDLESTICKS_ONE_MINUTE_WS = "@kline_1m"
AGGREGATED_TRADE_WS = "@aggTrade"
ATR_INDEX_SIZE = 2


async def binance_to_mongodb(multisocket_candle, candlestick_db, ta_lines_db, coin_ratio):
    time_counter = int(time.time())
    sp500_coin_volume = {}
    sp500_coin_present_values = {}
    sp500_marketcap_present_value = {}

    # Todo: falta o trigger que vai de 5 em 5 segundos inserir na BD o volume (falta fazer este), o RS ( acho
    # que já está tudo calculado ), e o preço atual.

    # Todo: tenho de fazer as candles de 5m do ATR e do volume, o ratio pode ser atualizado sem fazer reset ao fund
    # para "1.00",pois só mudam os ratios, os preços que subiram ou desceram continuam a fazer o valor ficar
    # consistente, atualizar o ratio várias vezes para ser desprezievel a mudança no ratio.

    atr = {}
    current_klines = {}

    async with multisocket_candle as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
                if int(time.time()) > time_counter + 2:
                    time_counter += 2
                    # TODO : #mongo.insert_in_db()
                if CANDLESTICK_WS in ws_trade['stream']:

                    kline_trade_data = ws_trade['data']['k']
                    clean_kline_data = {kline_trade_data['s']: dts.clean_data(kline_trade_data,
                                                                              't', 'v', 'o', 'h', 'l', 'c')}

                    current_klines, new_kline_data = dts.data_feed(current_klines, clean_kline_data)

                    if new_kline_data:
                        atr = dts.update_atr(new_kline_data)
                        await mongo.insert_in_db(candlestick_db, new_kline_data)

                elif AGGREGATED_TRADE_WS in ws_trade['stream']:
                    aggtrade_data = ws_trade['data']

                    symbol = aggtrade_data['s']
                    clean_symbol = dts.remove_usdt(symbol)
                    aggtrade_clean_data = dts.clean_data(aggtrade_data, 'E', 'p', 'q')

                    if symbol in sp500_symbols_usdt_pairs:
                        sp500_coin_present_values.update({clean_symbol: float(aggtrade_data['p'])})

                        if not clean_symbol in sp500_coin_volume:
                            sp500_coin_volume.update({clean_symbol: float(aggtrade_data['q'])})
                        else:
                            sp500_coin_volume[clean_symbol] += float(aggtrade_data['q'])

                        sp500_marketcap_present_value.update({clean_symbol: (float(aggtrade_data['p']) *
                                                                             coin_ratio[clean_symbol])})

                    await dts.insert_aggtrade_data(ta_lines_db, symbol, aggtrade_clean_data)

                print(f"{time.time()} , {ws_trade['data']['E']} , calc")

            except Exception as e:
                # TODO: treat restart when QUEUE reaches limit.
                print(f"{e}, {ws_trade}")


async def main():
    candlestick_db = mongo.connect_to_usdt_candlestick_db()
    ta_lines_db = mongo.connect_to_TA_lines_db()
    coin_normalized_ratio = dts.sp500_multiply_usdt_ratio(dts.remove_usdt(sp500_symbols_usdt_pairs),
                                                          coingecko_marketcap_api_link)

    binance_client = await AsyncClient.create()
    bm = BinanceSocketManager(binance_client)

    while True:
        await binance_to_mongodb(bm.multiplex_socket(
            dts.symbols_stream(CANDLESTICKS_ONE_MINUTE_WS) +
            dts.symbols_stream(AGGREGATED_TRADE_WS)),
            candlestick_db, ta_lines_db, coin_normalized_ratio)

# TODO: clean symbols that start with usdt and not finish with them, acho que é um erro do binance... mas a variavel das moedas
#  tem 39 simbolos e os dicts 38, verificar qual falta.

# TODO: criar candles do marketcap para fazer o seu ATR, já tenho o valor sempre atual, de 2 em 2 segundos meter na candle.
# TODO: when implementing ATR, create abstraction for 1m to 5m candles, DB needs all 1m candles, ATR will be mostly used with 5m candles.
# TODO: create indexs
# TODO: implement coingecko refresh 24h.
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
