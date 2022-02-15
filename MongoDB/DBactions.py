from motor.motor_asyncio import AsyncIOMotorClient as AsyncMotorClient
from pymongo import MongoClient

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
    return AsyncMotorClient('mongodb://localhost:27017/Relative_strength').get_default_database()


def connect_to_ta_lines_db():
    return AsyncMotorClient('mongodb://localhost:27017/TA_RS_VOL').get_default_database()


async def duplicate_insert_aggtrade_data(db, data: dict):
    insert_many_in_db(db, data)


async def duplicate_insert_data_rs_volume_price(db, data: dict):
    insert_many_in_db(db, data)


def insert_one_in_db(database, data):
    for key in list(data.keys()):
        database.get_collection(key).insert_one(data[key])


def insert_many_in_db(database, data):
    for key in list(data.keys()):
        database.get_collection(key).insert_many(data[key])

