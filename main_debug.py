'''The main entry point
'''
import os
import sys
from datetime import datetime, timedelta, timezone
from os import makedirs, path
from threading import Timer

import asyncio
from mathematician import config
from mathematician.pipe import PipeLine
from mathematician.Processor.HKMOOC2MongoProcessor import (fetch_data,
                                                           course,
                                                           db,
                                                           Config,
                                                           enrollment,
                                                           inject_root_user,
                                                           log,
                                                           rawfile,
                                                           user,
                                                           forum)


def seconds_to_tmr_1_am():
    '''Get the time interval to tomorrow 1am in sencends
    '''
    now = datetime.now(timezone(timedelta(hours=8)))
    tomorrow = now.replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)
    delta = tomorrow - now
    secs = delta.seconds + 1
    return secs


def app(first_time=False):
    '''The main entry of our script
    '''
    # create a new loop in this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    # Hong Kong Time
    now = datetime.now(timezone(timedelta(hours=8)))
    dir_name = str(now.year) + '-' + str(now.month) + '-' + str(now.day - 1)
    dir_name = os.path.join(".", Config.DATA_DIR, dir_name)
    if not path.exists(dir_name):
        makedirs(dir_name, exist_ok=True)
    start_time = datetime.now()
    download = fetch_data.DownloadFileFromServer(dir_name)
    file_names = download.get_files_to_be_processed()
    if len(file_names):
        pipeline = PipeLine()
        pipeline.input_files(file_names) \
            .pipe(course.CourseProcessor()) \
            .pipe(enrollment.EnrollmentProcessor()) \
            .pipe(log.LogProcessor()) \
            .pipe(user.UserProcessor()) \
            .pipe(rawfile.RawFileProcessor()) \
            .pipe(forum.ForumProcessor()) \
            .pipe(inject_root_user.InjectSuperUserProcessor("zhutian")) \
            .pipe(db.DumpToDB())
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
