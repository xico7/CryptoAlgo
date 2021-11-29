import requests as requests
from binance.client import Client
import logging
import threading
import time
from binance import AsyncClient, BinanceSocketManager

import MongoDB.DBactions as mdb


import asyncio
import motor.core


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()




async def binance_to_mongodb(ms, db):
    async with ms as tscm:
        while True:
            res = await tscm.recv()
            await mdb.do_insert(db, res)
            print(res)



async def main():
    db, client2 = mdb.connect_mongodb()

    client = await AsyncClient.create()
    bm = BinanceSocketManager(client)

    #await binance_to_mongodb(bm.multiplex_socket(new_list), db)
    loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(loop,), daemon=True)
    t.start()
    # await asyncio.gather(*[mdb.do_insert(database)])
    await binance_to_mongodb(bm.multiplex_socket(new_list), db)
    asyncio.run_coroutine_threadsafe(binance_to_mongodb(bm.multiplex_socket(new_list), db), loop)



# Press the green button in the gutter to run the script.
api_key = "7tmh1WvBaWmB2giUU3xCRbEJcQq19D9xrGz6yEwMllfTRNyE9IVW2Derk2rVyfV3"
api_secret = "4rWXYlGUkNs4FWRZlwJD9EQR6SlFXFkYYQAMY0gpOWH5hoGisRafob7PU9VJp99Z"
client = Client(api_key, api_secret)

link_binance_allsymbolsprice = "https://api.binance.com/api/v3/ticker/price"

USDT_symbols = []
json_binance_symbols_price = requests.get(link_binance_allsymbolsprice).json()

for x in range(len(json_binance_symbols_price)):
    if "USDT" in json_binance_symbols_price[x]['symbol']:
        USDT_symbols.append(json_binance_symbols_price[x]['symbol'])

lower_list = [each_string.lower() for each_string in USDT_symbols]
new_list = [x + "@aggTrade" for x in lower_list]

# TODO: if new coin is present, create index and collection, verify this behavior and execute in background.
# TODO: implement threading for other funcionts.. index etc.


if __name__ == "__main__":

    asyncio.run(main())
