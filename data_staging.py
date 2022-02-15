import re
import time
from typing import Union, List
from pymongo import MongoClient


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


#TODO: refactor
def create_last_day_rs_chart(timestamp, coin_current_price):
    timestamp_minus_one_day = timestamp - (60 * 60 * 24)
    db = MongoClient('mongodb://localhost:27017/')['Relative_strength']

    all_symbols_data_dict = {}
    for collection in db.list_collection_names():
        result = list(db.get_collection(collection).find({'$and': [
            {'Time': {'$gte': timestamp_minus_one_day}},
            {'Time': {'$lte': timestamp_minus_one_day + 864000}}
        ]
        }).rewind())
        symbol_data_dict = {}
        for elem in result:
            number = (elem['Price'] * 100 / coin_current_price[remove_usdt(collection)]) - 100
            if number > 0:
                counter = 0
                while number > 0:
                    number -= 0.5
                    counter += 1
                if counter not in symbol_data_dict:
                    symbol_data_dict.update({counter: {"Totalvolume": elem['v'], "Average_RS": elem['RS']}})
                else:
                    symbol_data_dict[counter]["Totalvolume"] += elem['v']
                    symbol_data_dict[counter]["Average_RS"] += elem['v'] / symbol_data_dict[counter]["Totalvolume"] * \
                                                               elem['RS']

            if number < 0:
                counter = 0
                while number < 0:
                    number += 0.5
                    counter -= 1
                if counter not in symbol_data_dict:
                    symbol_data_dict.update({counter: {"Totalvolume": elem['v'], "Average_RS": elem['RS']}})
                else:
                    symbol_data_dict[counter]["Totalvolume"] += elem['v']
                    symbol_data_dict[counter]["Average_RS"] += elem['v'] / symbol_data_dict[counter]["Totalvolume"] * \
                                                               elem['RS']

        all_symbols_data_dict[collection] = symbol_data_dict

    return all_symbols_data_dict


