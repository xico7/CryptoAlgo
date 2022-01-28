import copy

import motor.motor_asyncio
import asyncio
from pymongo import MongoClient
import time
import data_staging as dts

database_path = '/MongoDB'

def connect_to_usdt_candlestick_db():
    from motor.motor_asyncio import (
        AsyncIOMotorClient as MotorClient,
    )

    # MongoDB client
    client = MotorClient('mongodb://localhost:27017/BinanceUSDTCoinPairs')
    db = client.get_default_database()

    return db


def connect_to_RS_db():
    from motor.motor_asyncio import (
        AsyncIOMotorClient as MotorClient,
    )

    # MongoDB client
    client = MotorClient('mongodb://localhost:27017/Relative_strength')

    db = client.get_default_database()

    return db

def connect_to_TA_lines_db():
    from motor.motor_asyncio import (
        AsyncIOMotorClient as MotorClient,
    )

    # MongoDB client
    client = MotorClient('mongodb://localhost:27017/TA_RS_VOL')

    db = client.get_default_database()

    return db


async def insert_aggtrade_data_in_db(db, data: dict):

    # data = copy.deepcopy(data)
    #
    # db.create_collection(list(data.keys())[0])
    # database = db.get_collection(list(data.keys())[0])
    # await database.insert_one(data)

    for key in list(data.keys()):
        database = db.get_collection(key)
        database.insert_many(data[key])

async def insert_relative_strength_in_db(db, data: dict):
    pass
    # data = copy.deepcopy(data)
    #
    # db.create_collection(list(data.keys())[0])
    # database = db.get_collection(list(data.keys())[0])
    # await database.insert_one(data)

    for key in list(data.keys()):
        database = db.get_collection(key)
        database.insert_many(data[key])






