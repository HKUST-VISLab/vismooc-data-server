'''The main entry point
'''
import os
import sys
from datetime import datetime, timedelta, timezone
from os import makedirs, path
from threading import Timer

import asyncio
from mathematician import config
from mathematician.config import DBConfig as DBc
from mathematician.config import FilenameConfig as FC
from mathematician.DB import mongo_dbhelper
from mathematician.pipe import PipeLine
from mathematician.Processor.HKMOOC2MongoProcessor import (fetch_data, CourseProcessor,
                                                           DumpToDB,
                                                           EnrollmentProcessor,
                                                           InjectSuperUserProcessor,
                                                           LogProcessor,
                                                           RawFileProcessor,
                                                           UserProcessor)

def get_offline_files():
    """Fetch meta db files data from db"""
    database = mongo_dbhelper.MongoDB(DBc.DB_HOST, DBc.DB_NAME, DBc.DB_PORT)
    metadbfiles = database.get_collection(DBc.COLLECTION_METADBFILES).find({})
    items = []
    for item in metadbfiles:
        if item.get(DBc.FIELD_METADBFILES_TYPE) == DBc.TYPE_MONGO:
            file_path = item.get(DBc.FIELD_METADBFILES_FILEPATH)
            file_path = file_path[:file_path.rindex(os.sep)]
            mongo_files = [path.join(file_path, FC.ACTIVE_VERSIONS),
                           path.join(file_path, FC.STRUCTURES)]
        else:
            items.append(item)
    files = [item.get(DBc.FIELD_METADBFILES_FILEPATH) for item in items]
    return files + mongo_files


def seconds_to_tmr_1_am():
    '''Get the time interval to tomorrow 1am in sencends
    '''
    now = datetime.now(timezone(timedelta(hours=8)))
    tomorrow = now.replace(day=now.day + 1, hour=1,
                           minute=0, second=0, microsecond=0)
    delta = tomorrow - now
    secs = delta.seconds + 1
    return secs


def app(first_time=False, offline=False):
    '''The main entry of our script
    '''
    # create a new loop in this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Hong Kong Time
    now = datetime.now(timezone(timedelta(hours=8)))
    dir_name = str(now.year) + '-' + str(now.month) + '-' + str(now.day - 1)
    dir_name = os.path.join(".", config.FilenameConfig.Data_dir, dir_name)
    if not path.exists(dir_name):
        makedirs(dir_name, exist_ok=True)
    start_time = datetime.now()
    if offline:
        file_names = get_offline_files()
    else:
        download = fetch_data.DownloadFileFromServer(dir_name)
        file_names = download.get_files_to_be_processed()
    if len(file_names):
        pipeline = PipeLine()
        pipeline.input_files(file_names).pipe(CourseProcessor()).pipe(
            EnrollmentProcessor()).pipe(LogProcessor()).pipe(UserProcessor()).pipe(
                RawFileProcessor()).pipe(InjectSuperUserProcessor("zhutian")).pipe(DumpToDB())
        pipeline.execute()
    print('spend time:' + str(datetime.now() - start_time))
    if first_time:
        timer = Timer(seconds_to_tmr_1_am(), app)
    else:
        timer = Timer(60 * 60 * 24, app)
    timer.start()


def main():
    '''Entry point
    '''
    # init the config if config file is provided
    if len(sys.argv) >= 2:
        config.init_config(sys.argv[1])
    app(first_time=True)

if __name__ == "__main__":
    main()
