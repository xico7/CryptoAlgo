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

def connect_to_db():
    from motor.motor_asyncio import (
        AsyncIOMotorClient as MotorClient,
    )

    # MongoDB client
    client = MotorClient('mongodb://localhost:27017/BinanceUSDTCoinPairs')
    client.get_io_loop = asyncio.get_running_loop

    db = client.get_default_database()

    return db, client





async def insert_in_db(db, socket_data):

    db.create_collection(list(socket_data.keys())[0])

    database = db.get_collection(list(socket_data.keys())[0])
    await database.insert_one(socket_data)

    pass



