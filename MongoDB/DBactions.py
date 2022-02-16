from pymongo import MongoClient
#TODO: is this async or normal? depends on the queries.. their time etc,..


def connect_to_1m_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_1minutes').get_default_database()


def connect_to_5m_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_5minutes').get_default_database()


def connect_to_15m_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_15minutes').get_default_database()


def connect_to_1h_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_1hour').get_default_database()


def connect_to_4h_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_4hour').get_default_database()


def connect_to_1d_ohlc_db():
    return MongoClient('mongodb://localhost:27017/OHLC_1day').get_default_database()


def connect_to_rs_db():
    return MongoClient('mongodb://localhost:27017/Relative_strength').get_default_database()


def connect_to_ta_lines_db():
    return MongoClient('mongodb://localhost:27017/TA_RS_VOL').get_default_database()


