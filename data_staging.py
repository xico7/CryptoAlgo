import re
import time
from typing import Union, List

import numpy as np
import talib
from pymongo import MongoClient
import MongoDB.DBactions as mongo

def remove_usdt(symbols: Union[List[str], str]):
    if isinstance(symbols, str):
        try:
            return re.match('(^(.+?)USDT)', symbols).groups()[1].upper()
        except AttributeError as e:
            return None
    else:
        return [re.match('(^(.+?)USDT)', symbol).groups()[1].upper() for symbol in symbols]


def get_current_time() -> int:
    return int(time.time())


def create_last_days_rel_volume():
    DAYS_NUMBER = 7
    # TODO: 1 day instead of 4h db_feed = mongo.connect_to_1h_ohlc_db()
    # DONE 20fev 10:45
    db_oneday_feed = mongo.connect_to_1d_ohlc_db()

    coins_relative_volume = {}
    for collection in db_oneday_feed.list_collection_names():
        coins_last_days_volumes = 0
        for elem in list(db_oneday_feed.get_collection(collection).find(sort=[("Time", -1)]))[1:DAYS_NUMBER]:
            coins_last_days_volumes += float(elem['v']) / DAYS_NUMBER
        try:
            coins_relative_volume[collection] = list(db_oneday_feed.get_collection(collection).find(sort=[("Time", -1)]))[1]['v'] / coins_last_days_volumes
        except ZeroDivisionError:
            return 0
        except IndexError:
            return 0

    return coins_relative_volume

def create_14periods5min_rel_volume():
    db_5m_feed = mongo.connect_to_5m_ohlc_db()

    coins_relative_volume = {}
    for collection in db_5m_feed.list_collection_names():
        coins_last_14p5m_volumes = 0
        for elem in list(db_5m_feed.get_collection(collection).find(sort=[("Time", -1)]))[:14]:
            coins_last_14p5m_volumes += float(elem['v']) / 14
            #TODO : Review line 47 list index out of range.. maybe because i rebooted astarted both runs at same time and it was unpopulated.
        coins_relative_volume[collection] = list(db_5m_feed.get_collection(collection).find(sort=[("Time", -1)]))[1]['v'] / coins_last_14p5m_volumes

    return coins_relative_volume


def create_14periods5min_atrp():
    db_fiveminutes_feed = mongo.connect_to_5m_ohlc_db()

    coins_fiveminutes_atrp = {}
    for collection in db_fiveminutes_feed.list_collection_names():
        high, low, close = [], [], []
        for elem in list(db_fiveminutes_feed.get_collection(collection).find(sort=[("Time", -1)]))[:14]:
            high.append(elem['h'])
            low.append(elem['l'])
            close.append(elem['c'])

        coins_fiveminutes_atrp[collection] = talib.ATR(np.array(high), np.array(low), np.array(close), timeperiod=13)[13]

    return coins_fiveminutes_atrp


#TODO: refactor
def create_last_day_rs_chart(timestamp):
    timestamp_minus_one_day = timestamp - (60 * 60 * 24)
    db = MongoClient('mongodb://localhost:27017/')['Relative_strength']

    all_symbols_data_dict = {}
    coins_moment_prices = {}

    db_feed = mongo.connect_to_ta_lines_db()
    for collection in db_feed.list_collection_names():
        coins_moment_prices[collection] = float(db_feed.get_collection(collection).find_one(sort=[("E", -1)])['p'])
    for collection in db.list_collection_names():
        result = list(db.get_collection(collection).find({'$and': [
            {'Time': {'$gte': timestamp_minus_one_day}},
            {'Time': {'$lte': timestamp_minus_one_day + 864000}}
        ]
        }).rewind())
        symbol_data_dict = {}
        for elem in result:

            number = (elem['Price'] * 100 / coins_moment_prices[collection]) - 100
            if number > 0:
                counter = 0
                while number > 0:
                    number -= 0.5
                    counter += 1
                if counter not in symbol_data_dict:
                    symbol_data_dict.update({str(counter): {"Totalvolume": elem['v'], "Average_RS": elem['RS']}})
                else:
                    symbol_data_dict[str(counter)]["Totalvolume"] += elem['v']
                    symbol_data_dict[str(counter)]["Average_RS"] += elem['v'] / symbol_data_dict[counter]["Totalvolume"] * \
                                                               elem['RS']

            if number < 0:
                counter = 0
                while number < 0:
                    number += 0.5
                    counter -= 1
                if counter not in symbol_data_dict:
                    symbol_data_dict.update({str(counter): {"Totalvolume": elem['v'], "Average_RS": elem['RS']}})
                else:
                    symbol_data_dict[str(counter)]["Totalvolume"] += elem['v']
                    symbol_data_dict[str(counter)]["Average_RS"] += elem['v'] / symbol_data_dict[counter]["Totalvolume"] * \
                                                               elem['RS']

        all_symbols_data_dict[collection] = symbol_data_dict
    return all_symbols_data_dict


