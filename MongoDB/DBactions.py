import motor.motor_asyncio
import asyncio
from pymongo import MongoClient
import time

database_path = '/MongoDB'

# def create_db_Volume_fund():
#     conn = sqlite3.connect(database_path)
#
#     c = conn.cursor()
#
#     createtable = f"""CREATE TABLE crypto_fund (
#                      timestamp integer,
#                      price integer )"""
#     c.execute(createtable)
#
#     conn.commit()
#
#     conn.close()

def connect_mongodb():
    from motor.motor_asyncio import (
        AsyncIOMotorClient as MotorClient,
    )

    # MongoDB client
    client = MotorClient('mongodb://localhost:27017/BinanceUSDTCoinPairs')
    client.get_io_loop = asyncio.get_running_loop

    # The current database ("test")
    db = client.get_default_database()


    # db = client.test_database
    # collection = db.test_collection

    return db, client


#!/usr/bin/env python3
# countasync.py


async def do_insert(db, socket_data):
    my_collection = socket_data['data']['s']

    db.create_collection(my_collection)

    database = db.get_collection(my_collection)
#    database.createIndex(background: True)
    await database.insert_one(socket_data)

    pass



