import traceback
from pymongo.errors import ServerSelectionTimeoutError
import data_staging as dts
import MongoDB.DBactions as mongo
import asyncio
import logging



logging.basicConfig(level=logging.INFO)


class TACache:
    _ta_chart_value = {}
    _average_true_range_percentage = {}
    _relative_volume = {}

    @property
    def atrp(self):
        return self._average_true_range_percentage

    @atrp.setter
    def atrp(self, value):
        _atrp = value

    @property
    def rel_vol(self):
        return self._relative_volume

    @rel_vol.setter
    def rel_vol(self, value):
        _relative_volume = value

    @property
    def ta_chart(self):
        return self._ta_chart_value

    @ta_chart.setter
    def ta_chart(self, value):
        _ta_chart_value = value


PRINT_RUNNING_EXECUTION_EACH_SECONDS = 60


async def ta_analysis():
    debug_running_execution = begin_run = dts.get_current_time()
    ta_cache = TACache()
    current_minute = 0
    while True:
        try:

            cur_time = dts.get_current_time()
            while cur_time % 60 != 0:
                cur_time -= 1

            if current_minute != cur_time:
                current_minute = cur_time

                if current_minute % 1800 == 0:
                    # if finished_ohlc_open_timestamp % mongoDBcreate.THIRTY_MIN_IN_SEC == 0 and \
                    #         finished_ohlc_open_timestamp > (begin_run + mongoDBcreate.ONE_DAY_IN_SEC):
                    ta_cache.ta_chart = dts.create_last_day_rs_chart(current_minute, get_db_coins_momentprice)

            pass
            # TODO: Relative volume ATRP and Sinals here, after creating last day rs chart

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
