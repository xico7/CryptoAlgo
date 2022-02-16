import time
import traceback

from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
import data_staging as dts
import MongoDB.DBactions as mongo
import asyncio
import logging



logging.basicConfig(level=logging.INFO)


class TACache:
    _ta_chart_value = {}
    _relative_volume_long = {}
    _relative_volume_short = {}
    _atrp = {}

    @property
    def atrp(self):
        return self._atrp

    @atrp.setter
    def atrp(self, value):
        self._atrp = value

    @property
    def short_rel_vol(self):
        return self._relative_volume_short

    @short_rel_vol.setter
    def short_rel_vol(self, value):
        self._relative_volume_short = value

    @property
    def long_rel_vol(self):
        return self._relative_volume_long

    @long_rel_vol.setter
    def long_rel_vol(self, value):
        self._relative_volume_long = value

    @property
    def ta_chart(self):
        return self._ta_chart_value

    @ta_chart.setter
    def ta_chart(self, value):
        self._ta_chart_value = value


PRINT_RUNNING_EXECUTION_EACH_SECONDS = 60


async def ta_analysis():
    debug_running_execution = begin_run = dts.get_current_time()
    ta_cache = TACache()
    current_minute = 0
    done_minute = 0
    while True:
        try:

            parse_current_minute = dts.get_current_time()
            while parse_current_minute % 60 != 0:
                parse_current_minute -= 1

            if current_minute != parse_current_minute:
                current_minute = parse_current_minute

                if current_minute % 1800 == 0:
                # TODO: if current_minute % 1800 == 0:
                    # if finished_ohlc_open_timestamp % mongoDBcreate.THIRTY_MIN_IN_SEC == 0 and \
                    #         finished_ohlc_open_timestamp > (begin_run + mongoDBcreate.ONE_DAY_IN_SEC):


                        # pairs_ohlcs = {}
                        # for collection in query_db.list_collection_names():
                        #     trade_data = list(query_db.get_collection(collection).find({'$and': [
                        #         {TIME: {'$gte': ohlc_open_timestamp}},
                        #         {TIME: {'$lte': ohlc_open_timestamp + ohlc_seconds}}
                        #     ]
                        #     }).rewind())
                    ta_cache.ta_chart = dts.create_last_day_rs_chart(current_minute)

            if done_minute != current_minute:
                ta_cache.long_rel_vol = dts.create_last_days_rel_volume()
                a = {"timestamp": current_minute, "coin_volume": ta_cache.long_rel_vol}
                ta_cache.short_rel_vol = dts.create_14periods5min_rel_volume()
                ta_cache.atrp = dts.create_14periods5min_atrp()
                done_minute = current_minute

            cur_time = dts.get_current_time()
            if begin_run < cur_time:
                db = MongoClient('mongodb://localhost:27017/')['Relative_strength']
                coins_rs_volume = {}
                for collection in db.list_collection_names():
                    try:
                        coins_rs_volume[collection] = {
                            "RS": list(db.get_collection(collection).find(
                                {'$and': [{'Time': {'$gte': cur_time - 30}}, {'Time': {'$lte': cur_time + 1}}]}).rewind())[-1]['RS']}
                    except IndexError:
                        coins_rs_volume[collection] = {"RS": 0}
                begin_run += 2
            print(f"RUN {int(time.time())}")

            if dts.get_current_time() > (debug_running_execution + PRINT_RUNNING_EXECUTION_EACH_SECONDS):
                print(dts.get_current_time())
                debug_running_execution += PRINT_RUNNING_EXECUTION_EACH_SECONDS

        except ServerSelectionTimeoutError as e:
            if "localhost:27017" in e.args[0]:
                logging.exception("Cannot connect to mongo DB")
                raise
            else:
                logging.exception("Unexpected error")
                raise
        except Exception as e:
            traceback.print_exc()
            print(f"{e}")

            exit(1)


async def main():
    while True:
        try:
            await ta_analysis()
        except Exception as e:
            traceback.print_exc()
            print(f"{e}")
            exit(1)

# TODO: connect to dbs again on ta_analysis function, maybe change its name..
# TODO: does this still need async code?? maybe if mongoDB queries are slow

if __name__ == "__main__":
    asyncio.run(main())
