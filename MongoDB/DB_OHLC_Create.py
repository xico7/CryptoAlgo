from pymongo import MongoClient
import time
import MongoDB.DBactions as mongo

# TODO: insert_ohlc_1m takes 10-13 seconds, is or will this be na problem?

TIME = "Time"
PRICE = 'Price'
RELATIVE_STRENGTH = 'RS'
VOLUME = 'v'
CLIENT = MongoClient('mongodb://localhost:27017/')

# OHLC
OPEN = 'o'
CLOSE = 'c'
HIGH = 'h'
LOW = 'l'

ONE_MIN_IN_SEC = 60
FIVE_MIN_IN_SEC = ONE_MIN_IN_SEC * 5
FIFTEEN_MIN_IN_SEC = ONE_MIN_IN_SEC * 15
THIRTY_MIN_IN_SEC = FIFTEEN_MIN_IN_SEC * 2
ONE_HOUR_IN_SEC = ONE_MIN_IN_SEC * 60
FOUR_HOUR_IN_SEC = ONE_HOUR_IN_SEC * 4
ONE_DAY_IN_SEC = ONE_HOUR_IN_SEC * 24


def create_insert_ohlc_data(ohlc_open_timestamp, query_db, destination_db, ohlc_seconds,
                            ohlc_open=OPEN, ohlc_close=CLOSE, ohlc_high=HIGH, ohlc_low=LOW, debug=False):
    pairs_ohlcs = {}
    for collection in query_db.list_collection_names():
        trade_data = list(query_db.get_collection(collection).find({'$and': [
            {TIME: {'$gte': ohlc_open_timestamp}},
            {TIME: {'$lte': ohlc_open_timestamp + ohlc_seconds}}
        ]
        }).rewind())

        if trade_data:
            rs_sum = volume = high = 0
            low = 999999999999
            opening_value, closing_value = trade_data[0][ohlc_open], trade_data[-1][ohlc_close]

            for elem in trade_data:
                rs_sum += elem[RELATIVE_STRENGTH]
                volume += elem[VOLUME]
                if elem[ohlc_high] > high:
                    high = elem[ohlc_high]
                if elem[ohlc_low] < low:
                    low = elem[ohlc_low]

            pairs_ohlcs[collection] = {TIME: ohlc_open_timestamp, OPEN: opening_value, HIGH: high, LOW: low,
                                       CLOSE: closing_value, RELATIVE_STRENGTH: rs_sum / len(trade_data),
                                       VOLUME: volume}
    if debug:
        print(pairs_ohlcs)
        print(destination_db)
    mongo.insert_one_in_db(destination_db, pairs_ohlcs)


# open timestamp is the last finished candle opening time,
# exactly what we want for one minute candle but not really whats
# needed for the other ones where we must add one minute.
def insert_ohlc_data(open_timestamp, ohlc_1m_db, ohlc_5m_db, ohlc_15m_db, ohlc_1h_db, ohlc_4h_db, ohlc_1d_db):
    if open_timestamp % ONE_MIN_IN_SEC == 0:
        create_insert_ohlc_data(
            open_timestamp, CLIENT['Relative_strength'], ohlc_1m_db, ONE_MIN_IN_SEC, PRICE, PRICE, PRICE, PRICE)
    if open_timestamp % FIVE_MIN_IN_SEC == 0:
        create_insert_ohlc_data((open_timestamp - FIVE_MIN_IN_SEC), CLIENT['OHLC_1minutes'], ohlc_5m_db, FIVE_MIN_IN_SEC)
    if open_timestamp % FIFTEEN_MIN_IN_SEC == 0:
        create_insert_ohlc_data((open_timestamp - FIFTEEN_MIN_IN_SEC), CLIENT['OHLC_5minutes'], ohlc_15m_db, FIFTEEN_MIN_IN_SEC)
    if open_timestamp % ONE_HOUR_IN_SEC == 0:
        create_insert_ohlc_data((open_timestamp - ONE_HOUR_IN_SEC), CLIENT['OHLC_15minutes'], ohlc_1h_db, ONE_HOUR_IN_SEC)
    if open_timestamp % FOUR_HOUR_IN_SEC == 0:
        create_insert_ohlc_data((open_timestamp - FOUR_HOUR_IN_SEC), CLIENT['OHLC_1hour'], ohlc_4h_db, FOUR_HOUR_IN_SEC, debug=True)
    if open_timestamp % ONE_DAY_IN_SEC == 0:
        create_insert_ohlc_data((open_timestamp - ONE_DAY_IN_SEC), CLIENT['OHLC_4hour'], ohlc_1d_db, ONE_DAY_IN_SEC)
