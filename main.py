import time
import traceback

import requests
from pymongo.errors import ServerSelectionTimeoutError
import data_staging as dts
from binance import AsyncClient, BinanceSocketManager
import MongoDB.DBactions as mongo
import asyncio
import logging
import MongoDB.DB_OHLC_Create as mongoDBcreate


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


class DatabaseCache:
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
    _coins_rel_strength = {}

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

    @property
    def coins_rel_strength(self):
        return self._coins_rel_strength

    @coins_rel_strength.setter
    def coins_rel_strength(self, value):
        if value == {}:
            self._coins_rel_strength = {}
            return

        coin_symbol = list(value.keys())[0]
        coin_rel_strength = list(value.values())[0]

        if coin_symbol not in self._coins_rel_strength:
            self._coins_rel_strength.update({coin_symbol: [coin_rel_strength]})
        else:
            self._coins_rel_strength[coin_symbol].append(coin_rel_strength)


class TACache:
    _ta_chart_value = {}

    @property
    def ta_chart(self):
        return self._ta_chart_value

    @ta_chart.setter
    def ta_chart(self, value):
        _ta_chart_value = value


coingecko_marketcap_api_link = "https://api.coingecko.com/api/v3/coins/" \
                               "markets?vs_currency=usd&order=market_cap_desc&per_page=150&page=1&sparkline=false"
AGGTRADE_PYCACHE = 1000
RS_CACHE = 1500
ATOMIC_INSERT_TIME = 2
CANDLESTICK_WS = "kline"
CANDLESTICKS_ONE_MINUTE_WS = f"@{CANDLESTICK_WS}_1m"
AGGREGATED_TRADE_WS = "@aggTrade"
PRICE_P = 'p'
QUANTITY = 'q'
SYMBOL = 's'
EVENT_TIMESTAMP = 'E'
OHLC_CACHE_PERIODS = 70  # TODO: change to 70
REL_STRENGTH_PERIODS = OHLC_CACHE_PERIODS - 1
PRINT_RUNNING_EXECUTION_EACH_SECONDS = 60


async def binance_to_mongodb(multisocket_candle, coin_ratio, ta_lines_db, rel_strength_db,
                             ohlc_1m_db, ohlc_5m_db, ohlc_15m_db, ohlc_1h_db, ohlc_4h_db, ohlc_1d_db):
    initiate_time_counter = debug_running_execution = dts.get_current_time()
    db_cache = DatabaseCache()
    ta_cache = TACache()
    pycache_counter = 0
    rs_cache_counter = 0

    async with multisocket_candle as tscm:
        while True:
            try:
                ws_trade = await tscm.recv()
                cur_time = dts.get_current_time()
                if cur_time > initiate_time_counter + ATOMIC_INSERT_TIME and db_cache.marketcap_latest_timestamp > 0:
                    initiate_time_counter += ATOMIC_INSERT_TIME
                    db_cache.marketcap_current_ohlc = dts.update_current_marketcap_ohlc_data(
                        db_cache.marketcap_current_ohlc,
                        db_cache.marketcap_latest_timestamp,
                        db_cache.marketcap_sum)
                    if len(db_cache.marketcap_ohlc_data) == OHLC_CACHE_PERIODS:
                        db_cache.coins_rel_strength, rs_cache_counter = dts.update_relative_strength_cache(
                             db_cache.marketcap_ohlc_data, db_cache.coins_ohlc_data,
                             db_cache.coins_volume, db_cache.coins_moment_price, rs_cache_counter)
                        db_cache.coins_volume = {}
                        is_new_minute_ohlc = cur_time % 60 == 0 or (cur_time + 1) % 60 == 0 or (
                                cur_time - 1) % 60 == 0

                        if rs_cache_counter > RS_CACHE or is_new_minute_ohlc:
                            await mongo.duplicate_insert_data_rs_volume_price(rel_strength_db, db_cache.coins_rel_strength)
                            db_cache.coins_rel_strength = {}
                            rs_cache_counter = 0
                            if is_new_minute_ohlc:
                                finished_ohlc_open_timestamp = cur_time
                                while (finished_ohlc_open_timestamp - 3) % 60 != 0:
                                    finished_ohlc_open_timestamp -= 1
                                finished_ohlc_open_timestamp -= 3
                                mongoDBcreate.insert_ohlc_data(finished_ohlc_open_timestamp,
                                                               ohlc_1m_db, ohlc_5m_db, ohlc_15m_db,
                                                               ohlc_1h_db, ohlc_4h_db, ohlc_1d_db)

                            # t = Thread(target=mongoDBcreate.insert_ohlc_data, args=(finished_ohlc_open_timestamp,
                            #                                                       ohlc_1m_db, ohlc_5m_db, ohlc_15m_db,
                            #                                                       ohlc_1h_db, ohlc_4h_db, ohlc_1d_db, ))
                            # t.start()
                    #             #TODO: change: if finished_ohlc_open_timestamp % mongoDBcreate.THIRTY_MIN_IN_SEC == 0:  # \
                    #             if finished_ohlc_open_timestamp % 600 == 0:
                    #                 # TODO: ADD--> and finished_ohlc_open_timestamp > (begin_run + mongoDBcreate.ONE_DAY_IN_SEC):
                    #                 ta_cache.ta_chart = dts.create_last_day_rs_chart(finished_ohlc_open_timestamp,
                    #                                                                  db_cache.coins_moment_price)
                    #
                    # #TODO: Relative volume ATRP and Sinals here, after creating last day rs chart


                if CANDLESTICK_WS in ws_trade['stream']:
                    db_cache.coins_current_ohlcs, db_cache.coins_ohlc_data, db_cache.marketcap_ohlc_data, db_cache.marketcap_latest_timestamp = \
                        await dts.update_ohlc_cached_values(db_cache.coins_current_ohlcs,
                                                            ws_trade['data']['k'],
                                                            db_cache.coins_ohlc_data,
                                                            db_cache.marketcap_ohlc_data,
                                                            db_cache.marketcap_current_ohlc,
                                                            db_cache.marketcap_latest_timestamp)

                elif AGGREGATED_TRADE_WS in ws_trade['stream']:
                    pycache_counter += 1

                    aggtrade_data, symbol_pair = ws_trade['data'], ws_trade['data'][SYMBOL]
                    coin_moment_price, coin_moment_trade_quantity = float(aggtrade_data[PRICE_P]), float(
                        aggtrade_data[QUANTITY])
                    coin_symbol = dts.remove_usdt(symbol_pair)

                    if coin_symbol:
                        db_cache.coins_moment_price = dts.update_cached_coins_values(
                            db_cache.coins_moment_price, coin_symbol, coin_moment_price)
                        db_cache.coins_volume = dts.update_cached_coin_volumes(
                            db_cache.coins_volume, coin_symbol, coin_moment_trade_quantity)

                        if symbol_pair in SP500_SYMBOLS_USDT_PAIRS:
                            db_cache.marketcap_coins_value = dts.update_cached_marketcap_coins_value(
                                db_cache.marketcap_coins_value, coin_symbol, coin_moment_price, coin_ratio[coin_symbol])

                            db_cache.marketcap_sum = sum(list(db_cache.marketcap_coins_value.values()))

                    db_cache.aggtrade_data = {
                        symbol_pair: dts.clean_data(aggtrade_data, EVENT_TIMESTAMP, PRICE_P, QUANTITY)}

                    if pycache_counter > AGGTRADE_PYCACHE:
                        await mongo.duplicate_insert_aggtrade_data(ta_lines_db, db_cache.aggtrade_data)
                        # await dts.insert_aggtrade_data(ta_lines_db, cache.aggtrade_data)
                        db_cache.aggtrade_data = {}
                        pycache_counter -= AGGTRADE_PYCACHE

                if (int(time.time()) - int(str(ws_trade['data'][EVENT_TIMESTAMP])[:-3])) > 10:
                    print(f"{int(time.time())} , {int(str(ws_trade['data'][EVENT_TIMESTAMP])[:-3])} , calc, "
                          f"segundos de diferença: '{int(time.time()) - int(str(ws_trade['data'][EVENT_TIMESTAMP])[:-3])}'")
                debug_running_execution_current_time = dts.get_current_time
                if debug_running_execution_current_time() > (
                        debug_running_execution + PRINT_RUNNING_EXECUTION_EACH_SECONDS):
                    debug_running_execution += PRINT_RUNNING_EXECUTION_EACH_SECONDS
                    print(dts.get_current_time())



            except ServerSelectionTimeoutError as e:
                if "localhost:27017" in e.args[0]:
                    logging.exception("Cannot connect to mongo DB")
                    raise
                else:
                    logging.exception("Unexpected error")
                    raise
            except Exception as e:
                traceback.print_exc()
                print(f"{e}, {ws_trade}")

                if ws_trade['m'] == 'Queue overflow. Message not filled':
                    raise QueueOverflow

                exit(1)


async def main():
    bm = BinanceSocketManager(await AsyncClient.create())
    while True:
        try:
            await binance_to_mongodb(
                bm.multiplex_socket(
                    dts.usdt_symbols_stream(CANDLESTICKS_ONE_MINUTE_WS) + dts.usdt_symbols_stream(AGGREGATED_TRADE_WS)),
                dts.get_coin_fund_ratio(dts.remove_usdt(SP500_SYMBOLS_USDT_PAIRS),
                                        requests.get(coingecko_marketcap_api_link).json()),
                mongo.connect_to_ta_lines_db(),
                mongo.connect_to_rs_db(),
                mongo.connect_to_1m_ohlc_db(),
                mongo.connect_to_5m_ohlc_db(),
                mongo.connect_to_15m_ohlc_db(),
                mongo.connect_to_1h_ohlc_db(),
                mongo.connect_to_4h_ohlc_db(),
                mongo.connect_to_1d_ohlc_db(),
            )
        except QueueOverflow as e:
            pass
        except Exception as e:
            exit(1)


# TODO: clean symbols that start with usdt and not finish with them, acho que é um erro do binance... mas a variavel das moedas
#  tem 39 simbolos e os dicts 38, verificar qual falta.
# TODO: implement coingecko verification symbols for marketcap, how?
# TODO: implement coingecko refresh 24h.

if __name__ == "__main__":
    asyncio.run(main())
