from pymongo import MongoClient
import time
import MongoDB.DBactions as mongo

# TODO: insert_ohlc_1m takes 10-13 seconds, is or will this be na problem?

TIME = "Time"
HIGH = 'h'
LOW = 'l'
OPEN = 'o'
CLOSE = 'c'
PRICE = 'Price'
RELATIVE_STRENGTH = 'RS'
VOLUME = 'Volume'
CLIENT = MongoClient('mongodb://localhost:27017/')


def insert_ohlc_1m(open_timestamp, ohlc_1m_db):
    create_insert_ohlc_data(open_timestamp, CLIENT['Relative_strength'], ohlc_1m_db, 1, PRICE, PRICE, PRICE, PRICE)


def insert_ohlc_5m(open_timestamp, ohlc_5m_db):
    create_insert_ohlc_data(open_timestamp, CLIENT['OHLC_1minutes'], ohlc_5m_db, 5)


def insert_ohlc_15m(open_timestamp, ohlc_15m_db):
    create_insert_ohlc_data(open_timestamp, CLIENT['OHLC_5minutes'], ohlc_15m_db, 15)


def insert_ohlc_1h(open_timestamp, ohlc_1h_db):
    create_insert_ohlc_data(open_timestamp, CLIENT['OHLC_15minutes'], ohlc_1h_db, 60)


def insert_ohlc_4h(open_timestamp, ohlc_4h_db):
    create_insert_ohlc_data(open_timestamp, CLIENT['OHLC_1hour'], ohlc_4h_db, 240)


def insert_ohlc_1d(open_timestamp, ohlc_1d_db):
    create_insert_ohlc_data(open_timestamp, CLIENT['OHLC_4hour'], ohlc_1d_db, 1440)


def create_insert_ohlc_data(ohlc_open_timestamp, query_db, destination_db, ohlc_minutes,
                            ohlc_open=OPEN, ohlc_close=CLOSE, ohlc_high=HIGH, ohlc_low=LOW):
    pairs_ohlcs = {}
    print(time.time())
    for collection in query_db.list_collection_names():
        trade_data = list(query_db.get_collection(collection).find({'$and': [
            {TIME: {'$gte': ohlc_open_timestamp}},
            {TIME: {'$lte': ohlc_open_timestamp + (60 * ohlc_minutes)}}
        ]
        }).rewind())

        if trade_data:
            rs_sum = volume = high = 0
            low = 999999999999
            open, close = trade_data[0][ohlc_open], trade_data[-1][ohlc_close]

            for elem in trade_data:
                rs_sum += elem[RELATIVE_STRENGTH]
                volume += elem[VOLUME]
                if elem[ohlc_high] > high:
                    high = elem[ohlc_high]
                if elem[ohlc_low] < low:
                    low = elem[ohlc_low]

            rs_average = rs_sum / len(trade_data)
            pairs_ohlcs[collection] = {TIME: ohlc_open_timestamp, OPEN: open, HIGH: high, LOW: low, CLOSE: close,
                                       RELATIVE_STRENGTH: rs_average, VOLUME: volume}

    mongo.insert_one_in_db(destination_db, pairs_ohlcs)
    print(time.time())
    pass


def transform_ohlc(open_timestamp, ohlc_1m_db, ohlc_5m_db, ohlc_15m_db, ohlc_1h_db, ohlc_4h_db, ohlc_1d_db):

    one_min_in_sec = 60
    five_min_in_sec = one_min_in_sec * 5
    fifteen_min_in_sec = one_min_in_sec * 15
    one_hour_in_sec = one_min_in_sec * 60
    four_hour_in_sec = one_min_in_sec * one_hour_in_sec * 4
    one_day_in_sec = one_min_in_sec * one_hour_in_sec * 24

    if open_timestamp % one_min_in_sec == 0:
        insert_ohlc_1m(open_timestamp, ohlc_1m_db)
    if open_timestamp % five_min_in_sec == 0:
        insert_ohlc_5m((open_timestamp - five_min_in_sec + one_min_in_sec), ohlc_5m_db)
    if open_timestamp % fifteen_min_in_sec == 0:
        insert_ohlc_15m((open_timestamp - fifteen_min_in_sec + one_min_in_sec), ohlc_15m_db)
    if open_timestamp % one_hour_in_sec == 0:
        insert_ohlc_1h((open_timestamp - one_hour_in_sec + one_min_in_sec), ohlc_1h_db)
    if open_timestamp % four_hour_in_sec == 0:
        insert_ohlc_4h((open_timestamp - four_hour_in_sec + one_min_in_sec), ohlc_4h_db)
    if open_timestamp % one_day_in_sec == 0:
        insert_ohlc_1d((open_timestamp - one_day_in_sec + one_min_in_sec), ohlc_1d_db)


